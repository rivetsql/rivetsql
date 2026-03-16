"""Centralized Spark ↔ Arrow conversion with automatic Spark 4.0 native path detection.

Provides two public functions:
- ``spark_to_arrow(df, session)`` — Spark DataFrame → PyArrow Table
- ``arrow_to_spark(table, session)`` — PyArrow Table → Spark DataFrame

The module detects native Arrow support (Spark 4.0+) once per session and
routes to the optimal path automatically.  On Spark 3.x the pandas
intermediary path is used transparently.
"""

from __future__ import annotations

import logging
from typing import Any

import pyarrow

_logger = logging.getLogger(__name__)

# Session-keyed cache: id(session) → bool
_native_arrow_cache: dict[int, bool] = {}


def _supports_native_arrow(session: Any) -> bool:
    """Check whether *session* supports native Arrow APIs (Spark 4.0+).

    Detection creates a trivial DataFrame and checks for the ``toArrow``
    attribute.  The result is cached by ``id(session)`` so the probe runs
    at most once per session.

    Returns ``False`` (and logs a warning) if the probe raises.
    """
    sid = id(session)
    if sid in _native_arrow_cache:
        return _native_arrow_cache[sid]

    try:
        probe_df = session.createDataFrame([], schema="a: int")
        result = hasattr(probe_df, "toArrow")
    except Exception:
        _logger.warning(
            "Failed to detect native Arrow support; falling back to pandas path",
            exc_info=True,
        )
        result = False

    _native_arrow_cache[sid] = result
    return result


def clear_cache() -> None:
    """Clear the detection cache.  Useful for testing."""
    _native_arrow_cache.clear()


# ---------------------------------------------------------------------------
# Internal conversion helpers
# ---------------------------------------------------------------------------


def _to_arrow_native(df: Any) -> pyarrow.Table:
    """Convert Spark DataFrame → Arrow via ``df.toArrow()`` (Spark 4.0+).

    Handles the case where ``toArrow()`` returns a
    :class:`pyarrow.RecordBatchReader` by calling ``read_all()``.
    """
    result = df.toArrow()
    if isinstance(result, pyarrow.RecordBatchReader):
        result = result.read_all()
    return result  # type: ignore[no-any-return]


def _to_arrow_pandas(df: Any) -> pyarrow.Table:
    """Convert Spark DataFrame → Arrow via ``toPandas()`` + ``from_pandas`` (Spark 3.x)."""
    return pyarrow.Table.from_pandas(df.toPandas())


def _to_spark_native(session: Any, table: pyarrow.Table) -> Any:
    """Convert Arrow → Spark DataFrame via ``session.createDataFrame(table)`` (Spark 4.0+)."""
    return session.createDataFrame(table)


def _to_spark_pandas(session: Any, table: pyarrow.Table) -> Any:
    """Convert Arrow → Spark DataFrame via ``to_pandas()`` + ``createDataFrame`` (Spark 3.x).

    Derives a Spark schema from the Arrow schema so that column types are
    preserved (avoids ``CANNOT_DETERMINE_TYPE`` errors for null columns).
    """
    from pyspark.sql.pandas.types import from_arrow_schema  # type: ignore[import-untyped]

    spark_schema = from_arrow_schema(table.schema)
    pandas_df = table.to_pandas()
    return session.createDataFrame(pandas_df, schema=spark_schema)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def spark_to_arrow(df: Any, session: Any) -> pyarrow.Table:
    """Convert a Spark DataFrame to a PyArrow Table.

    Uses the native Arrow path on Spark 4.0+ and falls back to the pandas
    intermediary path on Spark 3.x.  If the native path raises, the pandas
    path is attempted as a fallback.
    """
    if _supports_native_arrow(session):
        try:
            return _to_arrow_native(df)
        except Exception:
            _logger.warning(
                "Native spark_to_arrow failed; falling back to pandas path",
                exc_info=True,
            )
    return _to_arrow_pandas(df)


def arrow_to_spark(table: pyarrow.Table, session: Any) -> Any:
    """Convert a PyArrow Table to a Spark DataFrame.

    Uses the native Arrow path on Spark 4.0+ and falls back to the pandas
    intermediary path on Spark 3.x.  If the native path raises, the pandas
    path is attempted as a fallback.
    """
    if _supports_native_arrow(session):
        try:
            return _to_spark_native(session, table)
        except Exception:
            _logger.warning(
                "Native arrow_to_spark failed; falling back to pandas path",
                exc_info=True,
            )
    return _to_spark_pandas(session, table)
