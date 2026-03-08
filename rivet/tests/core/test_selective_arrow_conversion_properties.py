"""Property-based tests: Selective Arrow Conversion (Property 6).

Property 6: Selective Arrow Conversion
  For any fused group with entry joints and a materials dictionary, the set of keys
  converted to Arrow tables shall be exactly the intersection of the materials
  dictionary keys and the union of upstream dependencies of the group's entry joints.
  When the group has no upstream dependencies, the converted set shall be empty.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import MagicMock

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

# ── Helpers ─────────────────────────────────────────────────────────────────────


def _compute_needed_keys(
    entry_joints: list[str],
    joints: list[str],
    joint_map: dict[str, object],
) -> set[str]:
    """Replicate the selective conversion logic from Executor._execute_group_success."""
    needed_keys: set[str] = set()
    for jn in entry_joints or joints:
        cj = joint_map.get(jn)
        if cj:
            needed_keys.update(cj.upstream)  # type: ignore[attr-defined]
    return needed_keys


def _selective_convert(
    materials: dict[str, object],
    needed_keys: set[str],
) -> dict[str, pa.Table]:
    """Replicate the filtered dict comprehension."""
    return {
        k: v.to_arrow()  # type: ignore[union-attr]
        for k, v in materials.items()
        if k in needed_keys
    }


@dataclass(frozen=True)
class _FakeCompiledJoint:
    name: str
    upstream: list[str] = field(default_factory=list)


# ── Strategies ──────────────────────────────────────────────────────────────────

_joint_name = st.text(
    alphabet=st.characters(whitelist_categories=("Ll",), whitelist_characters="_"),
    min_size=1,
    max_size=8,
)


@st.composite
def selective_conversion_scenario(draw: st.DrawFn):
    """Generate a random scenario with materials, entry joints, and upstream deps.

    Returns (material_keys, entry_joints, all_joints, joint_map) where:
    - material_keys: set of keys present in the materials dict
    - entry_joints: list of entry joint names (may be empty → falls back to all_joints)
    - all_joints: list of all joint names in the group
    - joint_map: dict mapping joint name → _FakeCompiledJoint with upstream lists
    """
    # Generate a pool of unique names for joints and materials
    all_names = draw(
        st.lists(_joint_name, min_size=2, max_size=10, unique=True)
    )

    # Split into material keys and joint names (with overlap allowed)
    n_materials = draw(st.integers(min_value=0, max_value=len(all_names)))
    material_keys = set(all_names[:n_materials])

    # Pick some names as joints in the group
    n_joints = draw(st.integers(min_value=1, max_value=len(all_names)))
    group_joints = all_names[:n_joints]

    # Pick a subset as entry joints (may be empty to test fallback)
    use_entry_joints = draw(st.booleans())
    if use_entry_joints and len(group_joints) > 0:
        n_entry = draw(st.integers(min_value=1, max_value=len(group_joints)))
        entry_joints = group_joints[:n_entry]
    else:
        entry_joints = []

    # Build joint_map: each joint gets a random subset of all_names as upstream
    joint_map: dict[str, _FakeCompiledJoint] = {}
    for jn in group_joints:
        upstream = draw(
            st.lists(st.sampled_from(all_names), min_size=0, max_size=len(all_names), unique=True)
        )
        # Filter out self-references
        upstream = [u for u in upstream if u != jn]
        joint_map[jn] = _FakeCompiledJoint(name=jn, upstream=upstream)

    return material_keys, entry_joints, group_joints, joint_map


# ── Property 6: Selective Arrow Conversion ──────────────────────────────────────


@given(scenario=selective_conversion_scenario())
@settings(max_examples=100)
def test_property6_converted_keys_equal_intersection(scenario) -> None:
    """Property 6: converted keys == intersection of materials keys and union of upstream deps."""
    material_keys, entry_joints, group_joints, joint_map = scenario

    needed_keys = _compute_needed_keys(entry_joints, group_joints, joint_map)

    # Build mock materials: each value has a to_arrow() that returns a small table
    dummy_table = pa.table({"x": [1]})
    materials: dict[str, MagicMock] = {}
    for k in material_keys:
        mock_ref = MagicMock()
        mock_ref.to_arrow.return_value = dummy_table
        materials[k] = mock_ref

    arrow_materials = _selective_convert(materials, needed_keys)

    expected_keys = material_keys & needed_keys
    assert set(arrow_materials.keys()) == expected_keys


@given(scenario=selective_conversion_scenario())
@settings(max_examples=100)
def test_property6_unreferenced_materials_not_converted(scenario) -> None:
    """Property 6: materials not in upstream deps are never converted (to_arrow not called)."""
    material_keys, entry_joints, group_joints, joint_map = scenario

    needed_keys = _compute_needed_keys(entry_joints, group_joints, joint_map)

    dummy_table = pa.table({"x": [1]})
    materials: dict[str, MagicMock] = {}
    for k in material_keys:
        mock_ref = MagicMock()
        mock_ref.to_arrow.return_value = dummy_table
        materials[k] = mock_ref

    _selective_convert(materials, needed_keys)

    for k, mock_ref in materials.items():
        if k not in needed_keys:
            mock_ref.to_arrow.assert_not_called()
        else:
            mock_ref.to_arrow.assert_called_once()


@given(scenario=selective_conversion_scenario())
@settings(max_examples=100)
def test_property6_no_upstream_yields_empty(scenario) -> None:
    """Property 6: when all entry joints have empty upstream, arrow_materials is empty."""
    material_keys, entry_joints, group_joints, joint_map = scenario

    # Override all joints to have empty upstream
    empty_joint_map = {
        jn: _FakeCompiledJoint(name=jn, upstream=[])
        for jn in joint_map
    }

    needed_keys = _compute_needed_keys(entry_joints, group_joints, empty_joint_map)
    assert needed_keys == set()

    dummy_table = pa.table({"x": [1]})
    materials: dict[str, MagicMock] = {}
    for k in material_keys:
        mock_ref = MagicMock()
        mock_ref.to_arrow.return_value = dummy_table
        materials[k] = mock_ref

    arrow_materials = _selective_convert(materials, needed_keys)
    assert arrow_materials == {}

    # Verify no to_arrow() calls were made
    for mock_ref in materials.values():
        mock_ref.to_arrow.assert_not_called()
