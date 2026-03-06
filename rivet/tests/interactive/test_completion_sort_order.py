"""Property test for CompletionEngine sort order.

Property 11: Completion sort order
Validates: Requirements 8.2, 8.3

Properties verified:
- Joint completions always appear before catalog table completions.
- Catalog table completions always appear before SQL keyword completions.
- Joint completions carry type icons (⚪/🔵/🟣/🟢) in their detail.
- Catalog table completions carry 📁 in their detail.
- Sort order is stable: joints < catalog tables < keywords.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.interactive.completions import CompletionEngine
from rivet_core.interactive.types import CompletionKind

_ident = st.from_regex(r"[a-z][a-z0-9_]{0,12}", fullmatch=True)
_joint_type = st.sampled_from(["source", "sql", "python", "sink"])


def _make_joint(name: str, joint_type: str) -> dict:
    return {"name": name, "joint_type": joint_type, "engine": None, "columns": None}


def _make_catalog_entry(catalog: str, schema: str, table: str) -> dict:
    return {"catalog": catalog, "schema": schema, "table": table, "columns": None}


class TestCompletionSortOrderProperty:
    """Property 11: Completion sort order — joints first, then catalog, then keywords."""

    @given(
        joint_names=st.lists(_ident, min_size=1, max_size=5, unique=True),
        joint_types=st.lists(_joint_type, min_size=1, max_size=5),
        catalog=_ident,
        schema=_ident,
        table=_ident,
    )
    @settings(max_examples=100)
    def test_joints_before_catalog_tables(
        self,
        joint_names: list[str],
        joint_types: list[str],
        catalog: str,
        schema: str,
        table: str,
    ) -> None:
        """All joint completions appear before any catalog table completion."""
        # Ensure no name collision between joints and catalog table
        if table in joint_names:
            return

        engine = CompletionEngine()
        joints = [
            _make_joint(name, joint_types[i % len(joint_types)])
            for i, name in enumerate(joint_names)
        ]
        engine.update_assembly(joints)
        engine.update_catalogs([_make_catalog_entry(catalog, schema, table)])

        # Use empty prefix to get all completions
        results = engine.complete("SELECT ", 7)

        joint_indices = [
            i for i, c in enumerate(results) if c.kind == CompletionKind.JOINT
        ]
        catalog_indices = [
            i for i, c in enumerate(results) if c.kind == CompletionKind.CATALOG_TABLE
        ]

        if joint_indices and catalog_indices:
            assert max(joint_indices) < min(catalog_indices), (
                f"Joint at index {max(joint_indices)} should come before "
                f"catalog table at index {min(catalog_indices)}"
            )

    @given(
        catalog=_ident,
        schema=_ident,
        table=_ident,
    )
    @settings(max_examples=100)
    def test_catalog_tables_before_keywords(
        self,
        catalog: str,
        schema: str,
        table: str,
    ) -> None:
        """All catalog table completions appear before any SQL keyword completion."""
        engine = CompletionEngine()
        engine.update_assembly([])
        engine.update_catalogs([_make_catalog_entry(catalog, schema, table)])

        results = engine.complete("SELECT ", 7)

        catalog_indices = [
            i for i, c in enumerate(results) if c.kind == CompletionKind.CATALOG_TABLE
        ]
        keyword_indices = [
            i for i, c in enumerate(results) if c.kind == CompletionKind.SQL_KEYWORD
        ]

        if catalog_indices and keyword_indices:
            assert max(catalog_indices) < min(keyword_indices), (
                f"Catalog table at index {max(catalog_indices)} should come before "
                f"keyword at index {min(keyword_indices)}"
            )

    @given(
        joint_name=_ident,
        joint_type=_joint_type,
    )
    @settings(max_examples=100)
    def test_joint_detail_contains_type_icon(
        self,
        joint_name: str,
        joint_type: str,
    ) -> None:
        """Joint completions carry a type icon in their detail field."""
        icon_map = {"source": "⚪", "sql": "🔵", "python": "🟣", "sink": "🟢"}
        engine = CompletionEngine()
        engine.update_assembly([_make_joint(joint_name, joint_type)])
        engine.update_catalogs([])

        results = engine.complete("SELECT ", 7)
        joint_completions = [c for c in results if c.kind == CompletionKind.JOINT]

        assert len(joint_completions) >= 1
        for c in joint_completions:
            if c.label == joint_name:
                expected_icon = icon_map.get(joint_type, "")
                assert expected_icon in (c.detail or ""), (
                    f"Expected icon '{expected_icon}' in detail '{c.detail}' "
                    f"for joint_type '{joint_type}'"
                )

    @given(
        catalog=_ident,
        schema=_ident,
        table=_ident,
    )
    @settings(max_examples=100)
    def test_catalog_table_detail_contains_folder_icon(
        self,
        catalog: str,
        schema: str,
        table: str,
    ) -> None:
        """Catalog table completions carry 📁 in their detail field."""
        engine = CompletionEngine()
        engine.update_assembly([])
        engine.update_catalogs([_make_catalog_entry(catalog, schema, table)])

        results = engine.complete("SELECT ", 7)
        catalog_completions = [
            c for c in results if c.kind == CompletionKind.CATALOG_TABLE
        ]

        assert len(catalog_completions) >= 1
        for c in catalog_completions:
            assert "📁" in (c.detail or ""), (
                f"Expected '📁' in detail '{c.detail}' for catalog table '{c.label}'"
            )

    @given(
        joint_names=st.lists(_ident, min_size=1, max_size=3, unique=True),
        joint_types=st.lists(_joint_type, min_size=1, max_size=3),
        catalog=_ident,
        schema=_ident,
        table=_ident,
    )
    @settings(max_examples=100)
    def test_full_sort_order_joints_catalog_keywords(
        self,
        joint_names: list[str],
        joint_types: list[str],
        catalog: str,
        schema: str,
        table: str,
    ) -> None:
        """Full sort order: joints < catalog tables < keywords (no interleaving)."""
        if table in joint_names:
            return

        engine = CompletionEngine()
        joints = [
            _make_joint(name, joint_types[i % len(joint_types)])
            for i, name in enumerate(joint_names)
        ]
        engine.update_assembly(joints)
        engine.update_catalogs([_make_catalog_entry(catalog, schema, table)])

        results = engine.complete("SELECT ", 7)

        # Assign tier: 0=joint, 1=catalog, 2=keyword, 3=other
        def tier(kind: CompletionKind) -> int:
            if kind == CompletionKind.JOINT:
                return 0
            if kind in (CompletionKind.CATALOG_TABLE, CompletionKind.CATALOG_NAME):
                return 1
            if kind == CompletionKind.SQL_KEYWORD:
                return 2
            return 3

        tiers = [tier(c.kind) for c in results]
        # Tiers must be non-decreasing
        assert tiers == sorted(tiers), (
            f"Completion tiers not sorted: {tiers}"
        )
