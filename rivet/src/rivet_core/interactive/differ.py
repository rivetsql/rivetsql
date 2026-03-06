"""Arrow table diffing service."""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from rivet_core.interactive.types import ChangedRow, DiffResult


class Differ:
    """Computes DiffResult between two Arrow tables."""

    def diff(
        self,
        baseline: pa.Table,
        current: pa.Table,
        key_columns: list[str] | None = None,
    ) -> DiffResult:
        """Compare tables. Matches rows by key columns (default: first column).
        Falls back to positional comparison when no natural key exists."""
        if baseline.num_rows == 0 and current.num_rows == 0:
            return DiffResult(
                added=current.slice(0, 0),
                removed=baseline.slice(0, 0),
                changed=[],
                unchanged_count=0,
                key_columns=key_columns or [],
            )

        # Determine key columns
        # key_columns=None → default to first column
        # key_columns=[]   → explicit positional comparison
        if key_columns is None:
            keys = [baseline.schema.field(0).name] if baseline.num_columns > 0 else []
        else:
            keys = key_columns

        # Use positional comparison when no key columns
        if not keys:
            return self._positional_diff(baseline, current)

        return self._keyed_diff(baseline, current, keys)

    def _row_to_dict(self, table: pa.Table, idx: int) -> dict[str, Any]:
        return {col: table.column(col)[idx].as_py() for col in table.schema.names}

    def _row_key(self, table: pa.Table, idx: int, keys: list[str]) -> tuple[Any, ...]:
        return tuple(table.column(k)[idx].as_py() for k in keys)

    def _keyed_diff(
        self, baseline: pa.Table, current: pa.Table, keys: list[str]
    ) -> DiffResult:
        # Build index: key → row index for each table
        baseline_index: dict[tuple[Any, ...], int] = {}
        for i in range(baseline.num_rows):
            k = self._row_key(baseline, i, keys)
            baseline_index[k] = i

        current_index: dict[tuple[Any, ...], int] = {}
        for i in range(current.num_rows):
            k = self._row_key(current, i, keys)
            current_index[k] = i

        added_indices: list[int] = []
        removed_indices: list[int] = []
        changed: list[ChangedRow] = []
        unchanged_count = 0

        # Keys only in current → added
        for k, idx in current_index.items():
            if k not in baseline_index:
                added_indices.append(idx)

        # Keys only in baseline → removed; keys in both → changed or unchanged
        for k, b_idx in baseline_index.items():
            if k not in current_index:
                removed_indices.append(b_idx)
            else:
                c_idx = current_index[k]
                b_row = self._row_to_dict(baseline, b_idx)
                c_row = self._row_to_dict(current, c_idx)
                diffs = {
                    col: (b_row[col], c_row[col])
                    for col in baseline.schema.names
                    if col in c_row and b_row[col] != c_row[col]
                }
                if diffs:
                    key_dict = {kc: b_row[kc] for kc in keys}
                    changed.append(ChangedRow(key=key_dict, changes=diffs))
                else:
                    unchanged_count += 1

        added = current.take(added_indices) if added_indices else current.slice(0, 0)
        removed = baseline.take(removed_indices) if removed_indices else baseline.slice(0, 0)

        return DiffResult(
            added=added,
            removed=removed,
            changed=changed,
            unchanged_count=unchanged_count,
            key_columns=keys,
        )

    def _positional_diff(self, baseline: pa.Table, current: pa.Table) -> DiffResult:
        """Compare row-by-row by position when no key columns exist."""
        min_rows = min(baseline.num_rows, current.num_rows)
        changed: list[ChangedRow] = []
        unchanged_count = 0

        for i in range(min_rows):
            b_row = self._row_to_dict(baseline, i)
            c_row = self._row_to_dict(current, i)
            diffs = {
                col: (b_row[col], c_row[col])
                for col in baseline.schema.names
                if col in c_row and b_row[col] != c_row[col]
            }
            if diffs:
                changed.append(ChangedRow(key={"_pos": i}, changes=diffs))
            else:
                unchanged_count += 1

        added = current.slice(min_rows) if current.num_rows > min_rows else current.slice(0, 0)
        removed = baseline.slice(min_rows) if baseline.num_rows > min_rows else baseline.slice(0, 0)

        return DiffResult(
            added=added,
            removed=removed,
            changed=changed,
            unchanged_count=unchanged_count,
            key_columns=[],
        )
