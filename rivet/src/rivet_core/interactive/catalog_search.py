"""Fuzzy catalog search service.

Builds an in-memory index from catalogs and joints, then provides
ranked CatalogSearchResult items with match positions for highlighting.

Requirements: 6.2, 6.3
"""

from __future__ import annotations

from typing import Literal

from rivet_core.interactive.types import CatalogSearchResult


def _fuzzy_match(query: str, text: str) -> list[int] | None:
    """Subsequence fuzzy match. Returns match positions or None if no match."""
    q = query.lower()
    t = text.lower()
    positions: list[int] = []
    qi = 0
    for ti, ch in enumerate(t):
        if qi < len(q) and ch == q[qi]:
            positions.append(ti)
            qi += 1
    if qi == len(q):
        return positions
    return None


def _score(query: str, text: str, positions: list[int]) -> float:
    """Lower score = better match. Rewards contiguous runs and prefix matches."""
    if not positions:
        return float("inf")
    # Exact match bonus
    if query.lower() == text.lower():
        return -1.0
    # Contiguous bonus: count consecutive pairs
    consecutive = sum(1 for a, b in zip(positions, positions[1:]) if b == a + 1)
    prefix_bonus = 1.0 if positions[0] == 0 else 0.0
    # Base score: ratio of matched span to text length
    span = positions[-1] - positions[0] + 1
    base = span / max(len(text), 1)
    return base - consecutive * 0.1 - prefix_bonus * 0.2


class _IndexEntry:
    __slots__ = ("kind", "qualified_name", "short_name", "parent")

    def __init__(
        self,
        kind: Literal["catalog", "schema", "table", "column", "joint"],
        qualified_name: str,
        short_name: str,
        parent: str | None,
    ) -> None:
        self.kind = kind
        self.qualified_name = qualified_name
        self.short_name = short_name
        self.parent = parent


class CatalogSearch:
    """Fuzzy search across catalogs, tables, columns, and joints.

    Call update() to rebuild the index, then search() to query it.
    """

    def __init__(self) -> None:
        self._index: list[_IndexEntry] = []

    def update(
        self,
        catalog_entries: list[dict],  # type: ignore[type-arg]
        joint_names: list[str],
    ) -> None:
        """Rebuild the search index.

        catalog_entries is a list of dicts with keys:
          - catalog: str
          - schema: str | None
          - table: str | None
          - columns: list[str] | None  (column names for the table)

        joint_names is a list of joint name strings.
        """
        entries: list[_IndexEntry] = []

        seen_catalogs: set[str] = set()
        seen_schemas: set[tuple[str, str]] = set()

        for item in catalog_entries:
            catalog: str = item["catalog"]
            schema: str | None = item.get("schema")
            table: str | None = item.get("table")
            columns: list[str] | None = item.get("columns")

            if catalog not in seen_catalogs:
                entries.append(_IndexEntry("catalog", catalog, catalog, None))
                seen_catalogs.add(catalog)

            if schema is not None:
                schema_key = (catalog, schema)
                if schema_key not in seen_schemas:
                    qualified_schema = f"{catalog}.{schema}"
                    entries.append(
                        _IndexEntry("schema", qualified_schema, schema, catalog)
                    )
                    seen_schemas.add(schema_key)

                if table is not None:
                    qualified_table = f"{catalog}.{schema}.{table}"
                    entries.append(
                        _IndexEntry("table", qualified_table, table, f"{catalog}.{schema}")
                    )

                    if columns:
                        for col in columns:
                            qualified_col = f"{catalog}.{schema}.{table}.{col}"
                            entries.append(
                                _IndexEntry("column", qualified_col, col, qualified_table)
                            )

        for joint_name in joint_names:
            entries.append(_IndexEntry("joint", joint_name, joint_name, None))

        self._index = entries

    def search(self, query: str, limit: int = 50) -> list[CatalogSearchResult]:
        """Return ranked CatalogSearchResult items matching query.

        Matches against both qualified_name and short_name; returns the
        best match positions for highlighting. Results are sorted by score
        (lower = better).
        """
        if not query:
            return []

        results: list[tuple[float, CatalogSearchResult]] = []

        for entry in self._index:
            # Try short name first (preferred for display)
            short_positions = _fuzzy_match(query, entry.short_name)
            qual_positions = _fuzzy_match(query, entry.qualified_name)

            if short_positions is None and qual_positions is None:
                continue

            if short_positions is not None:
                score = _score(query, entry.short_name, short_positions)
                positions = short_positions
            else:
                # qual_positions is not None
                assert qual_positions is not None
                score = _score(query, entry.qualified_name, qual_positions)
                positions = qual_positions

            results.append(
                (
                    score,
                    CatalogSearchResult(
                        kind=entry.kind,
                        qualified_name=entry.qualified_name,
                        short_name=entry.short_name,
                        parent=entry.parent,
                        match_positions=positions,
                        score=score,
                    ),
                )
            )

        results.sort(key=lambda x: x[0])
        return [r for _, r in results[:limit]]
