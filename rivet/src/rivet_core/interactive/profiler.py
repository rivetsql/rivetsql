"""Column-level statistics via PyArrow compute kernels."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from rivet_core.interactive.types import ColumnProfile, ProfileResult


def _is_numeric(dtype: pa.DataType) -> bool:
    return pa.types.is_integer(dtype) or pa.types.is_floating(dtype) or pa.types.is_decimal(dtype)  # type: ignore[no-any-return]


def _is_temporal(dtype: pa.DataType) -> bool:
    return (  # type: ignore[no-any-return]
        pa.types.is_timestamp(dtype)
        or pa.types.is_date(dtype)
        or pa.types.is_time(dtype)
        or pa.types.is_duration(dtype)
    )


def _safe_scalar(val: pa.Scalar | None) -> object:
    if val is None or not val.is_valid:
        return None
    return val.as_py()


def _histogram(col: pa.ChunkedArray, bins: int = 8) -> list[int] | None:
    non_null = pc.drop_null(col)
    if len(non_null) == 0:
        return None
    if _is_temporal(col.type):
        if pa.types.is_date(col.type):
            non_null = non_null.cast(pa.timestamp("us")).cast(pa.int64())
        elif pa.types.is_time(col.type):
            non_null = non_null.cast(pa.int64())
        else:
            non_null = non_null.cast(pa.int64())
    min_val = pc.min(non_null).as_py()
    max_val = pc.max(non_null).as_py()
    if min_val is None or max_val is None or min_val == max_val:
        return [len(non_null)] + [0] * (bins - 1)
    [min_val + i * (max_val - min_val) / bins for i in range(bins + 1)]
    counts = [0] * bins
    for chunk in non_null.chunks:
        arr = chunk.to_pylist()
        for v in arr:
            idx = int((v - min_val) / (max_val - min_val) * bins)
            if idx >= bins:
                idx = bins - 1
            counts[idx] += 1
    return counts


def _profile_numeric(name: str, col: pa.ChunkedArray, num_rows: int) -> ColumnProfile:
    null_count = col.null_count
    null_pct = (null_count / num_rows * 100.0) if num_rows > 0 else 0.0
    distinct_count = pc.count_distinct(col, mode="all").as_py()
    float_col = pc.cast(col, pa.float64(), safe=False) if not pa.types.is_floating(col.type) else col
    return ColumnProfile(
        name=name,
        dtype=str(col.type),
        null_count=null_count,
        null_pct=null_pct,
        distinct_count=distinct_count,
        min=_safe_scalar(pc.min(col)),
        max=_safe_scalar(pc.max(col)),
        mean=_safe_scalar(pc.mean(float_col)),  # type: ignore[arg-type]
        median=_safe_scalar(pc.approximate_median(float_col)),  # type: ignore[arg-type]
        stddev=_safe_scalar(pc.stddev(float_col)),  # type: ignore[arg-type]
        histogram=_histogram(col),
        top_values=None,
    )


def _profile_string(name: str, col: pa.ChunkedArray, num_rows: int) -> ColumnProfile:
    null_count = col.null_count
    null_pct = (null_count / num_rows * 100.0) if num_rows > 0 else 0.0
    distinct_count = pc.count_distinct(col, mode="all").as_py()
    lengths = pc.utf8_length(col)
    value_counts = pc.value_counts(col)
    sorted_vc = pc.sort_indices(value_counts, sort_keys=[("counts", "descending")])
    top_values: list[tuple[object, int]] = []
    for i in range(min(5, len(sorted_vc))):
        entry = value_counts[sorted_vc[i].as_py()]
        val = entry["values"].as_py()
        cnt = entry["counts"].as_py()
        if val is not None:
            top_values.append((val, cnt))
    return ColumnProfile(
        name=name,
        dtype=str(col.type),
        null_count=null_count,
        null_pct=null_pct,
        distinct_count=distinct_count,
        min=_safe_scalar(pc.min(lengths)),
        max=_safe_scalar(pc.max(lengths)),
        mean=_safe_scalar(pc.mean(pc.cast(lengths, pa.float64()))),  # type: ignore[arg-type]
        median=None,
        stddev=None,
        histogram=None,
        top_values=top_values if top_values else None,
    )


def _profile_boolean(name: str, col: pa.ChunkedArray, num_rows: int) -> ColumnProfile:
    null_count = col.null_count
    null_pct = (null_count / num_rows * 100.0) if num_rows > 0 else 0.0
    non_null = num_rows - null_count
    true_count = pc.sum(col).as_py() or 0
    false_count = non_null - true_count
    top_values: list[tuple[object, int]] = [(True, true_count), (False, false_count)]
    return ColumnProfile(
        name=name,
        dtype=str(col.type),
        null_count=null_count,
        null_pct=null_pct,
        distinct_count=pc.count_distinct(col, mode="all").as_py(),
        min=None,
        max=None,
        mean=None,
        median=None,
        stddev=None,
        histogram=None,
        top_values=top_values,
    )


def _profile_temporal(name: str, col: pa.ChunkedArray, num_rows: int) -> ColumnProfile:
    null_count = col.null_count
    null_pct = (null_count / num_rows * 100.0) if num_rows > 0 else 0.0
    distinct_count = pc.count_distinct(col, mode="all").as_py()
    return ColumnProfile(
        name=name,
        dtype=str(col.type),
        null_count=null_count,
        null_pct=null_pct,
        distinct_count=distinct_count,
        min=_safe_scalar(pc.min(col)),
        max=_safe_scalar(pc.max(col)),
        mean=None,
        median=None,
        stddev=None,
        histogram=_histogram(col),
        top_values=None,
    )


class Profiler:
    """Computes ProfileResult for an Arrow table."""

    def profile(self, table: pa.Table) -> ProfileResult:
        """Compute per-column statistics dispatched by column type."""
        num_rows = table.num_rows
        columns: list[ColumnProfile] = []
        for i in range(table.num_columns):
            name = table.column_names[i]
            col = table.column(i)
            dtype = col.type
            if pa.types.is_boolean(dtype):
                columns.append(_profile_boolean(name, col, num_rows))
            elif _is_numeric(dtype):
                columns.append(_profile_numeric(name, col, num_rows))
            elif pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
                columns.append(_profile_string(name, col, num_rows))
            elif _is_temporal(dtype):
                columns.append(_profile_temporal(name, col, num_rows))
            else:
                # Fallback: basic stats only
                null_count = col.null_count
                columns.append(ColumnProfile(
                    name=name,
                    dtype=str(dtype),
                    null_count=null_count,
                    null_pct=(null_count / num_rows * 100.0) if num_rows > 0 else 0.0,
                    distinct_count=pc.count_distinct(col, mode="all").as_py(),
                    min=None, max=None, mean=None, median=None, stddev=None,
                    histogram=None, top_values=None,
                ))
        return ProfileResult(row_count=num_rows, column_count=table.num_columns, columns=columns)
