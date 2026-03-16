"""Unit tests for arrow_converter version detection and conversion path selection.

Covers:
- _supports_native_arrow detection (True/False/exception fallback/caching)
- spark_to_arrow path selection (native vs pandas, fallback)
- arrow_to_spark path selection (native vs pandas, fallback)
- RecordBatchReader unwrapping
- Deferred imports (no Spark 4.0-only symbols at module level)
"""

from __future__ import annotations

import logging
import sys
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from rivet_pyspark.arrow_converter import (
    _supports_native_arrow,
    arrow_to_spark,
    clear_cache,
    spark_to_arrow,
)


@pytest.fixture(autouse=True)
def _clear_detection_cache() -> None:
    """Reset the detection cache before each test."""
    clear_cache()


def _make_session(*, native: bool, create_raises: bool = False) -> MagicMock:
    """Build a mock SparkSession.

    Parameters
    ----------
    native:
        If True, the probe DataFrame will have a ``toArrow`` attribute.
    create_raises:
        If True, ``session.createDataFrame`` raises during the detection probe.
    """
    session = MagicMock()
    if create_raises:
        session.createDataFrame.side_effect = RuntimeError("probe failed")
        return session

    probe_df = MagicMock()
    if not native:
        del probe_df.toArrow  # remove the attribute so hasattr returns False

    session.createDataFrame.return_value = probe_df
    return session


# ---------------------------------------------------------------------------
# _supports_native_arrow
# ---------------------------------------------------------------------------


class TestSupportsNativeArrow:
    def test_returns_true_when_toArrow_present(self) -> None:
        session = _make_session(native=True)
        assert _supports_native_arrow(session) is True

    def test_returns_false_when_toArrow_absent(self) -> None:
        session = _make_session(native=False)
        assert _supports_native_arrow(session) is False

    def test_exception_fallback_returns_false_and_logs(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        session = _make_session(native=False, create_raises=True)
        with caplog.at_level(logging.WARNING, logger="rivet_pyspark.arrow_converter"):
            result = _supports_native_arrow(session)
        assert result is False
        assert "falling back to pandas" in caplog.text.lower()

    def test_caching_avoids_repeated_detection(self) -> None:
        session = _make_session(native=True)
        assert _supports_native_arrow(session) is True
        # Reset the side-effect to track a second call
        session.createDataFrame.reset_mock()
        assert _supports_native_arrow(session) is True
        session.createDataFrame.assert_not_called()


# ---------------------------------------------------------------------------
# spark_to_arrow
# ---------------------------------------------------------------------------


class TestSparkToArrow:
    def test_native_path_calls_toArrow(self) -> None:
        session = _make_session(native=True)
        df = MagicMock()
        expected_table = pa.table({"x": [1, 2]})
        df.toArrow.return_value = expected_table

        result = spark_to_arrow(df, session)

        df.toArrow.assert_called_once()
        df.toPandas.assert_not_called()
        assert result is expected_table

    def test_pandas_path_when_not_native(self) -> None:
        session = _make_session(native=False)
        df = MagicMock()
        expected_table = pa.table({"x": [1]})

        with patch(
            "rivet_pyspark.arrow_converter._to_arrow_pandas",
            return_value=expected_table,
        ) as mock_pandas:
            result = spark_to_arrow(df, session)

        mock_pandas.assert_called_once_with(df)
        assert result is expected_table

    def test_record_batch_reader_unwrapped(self) -> None:
        session = _make_session(native=True)
        df = MagicMock()
        expected_table = pa.table({"x": [1]})
        reader = MagicMock(spec=pa.RecordBatchReader)
        reader.read_all.return_value = expected_table
        df.toArrow.return_value = reader

        result = spark_to_arrow(df, session)

        reader.read_all.assert_called_once()
        assert result is expected_table

    def test_native_failure_falls_back_to_pandas(self, caplog: pytest.LogCaptureFixture) -> None:
        session = _make_session(native=True)
        df = MagicMock()
        df.toArrow.side_effect = RuntimeError("native boom")
        expected_table = pa.table({"x": [1]})

        with (
            caplog.at_level(logging.WARNING, logger="rivet_pyspark.arrow_converter"),
            patch(
                "rivet_pyspark.arrow_converter._to_arrow_pandas",
                return_value=expected_table,
            ),
        ):
            result = spark_to_arrow(df, session)

        assert result is expected_table
        assert "falling back to pandas" in caplog.text.lower()


# ---------------------------------------------------------------------------
# arrow_to_spark
# ---------------------------------------------------------------------------


def _patch_from_arrow_schema() -> Any:
    """Patch the deferred ``from pyspark.sql.pandas.types import from_arrow_schema``.

    Since pyspark may not be installed, we inject a mock module into
    ``sys.modules`` so the deferred import inside ``_to_spark_pandas`` succeeds.
    """
    mock_module = MagicMock()
    return patch.dict(sys.modules, {"pyspark.sql.pandas.types": mock_module})


class TestArrowToSpark:
    def test_native_path_calls_createDataFrame_with_table(self) -> None:
        session = _make_session(native=True)
        table = pa.table({"x": [1, 2]})
        expected_df = MagicMock()
        # First call is the detection probe; second is the actual conversion
        probe_df = session.createDataFrame.return_value
        session.createDataFrame.side_effect = [probe_df, expected_df]
        clear_cache()

        result = arrow_to_spark(table, session)

        assert session.createDataFrame.call_count == 2
        assert session.createDataFrame.call_args_list[1][0][0] is table
        assert result is expected_df

    def test_pandas_path_when_not_native(self) -> None:
        session = _make_session(native=False)
        table = pa.table({"x": pa.array([1, 2], type=pa.int64())})
        expected_df = MagicMock()

        with _patch_from_arrow_schema():
            session.createDataFrame.return_value = expected_df
            result = arrow_to_spark(table, session)

        assert result is expected_df

    def test_native_failure_falls_back_to_pandas(self, caplog: pytest.LogCaptureFixture) -> None:
        session = _make_session(native=True)
        table = pa.table({"x": pa.array([1], type=pa.int64())})
        expected_df = MagicMock()

        probe_df = session.createDataFrame.return_value
        # probe (ok) → native conversion (fails) → pandas fallback (ok)
        session.createDataFrame.side_effect = [
            probe_df,
            RuntimeError("native boom"),
            expected_df,
        ]
        clear_cache()

        with (
            caplog.at_level(logging.WARNING, logger="rivet_pyspark.arrow_converter"),
            _patch_from_arrow_schema(),
        ):
            result = arrow_to_spark(table, session)

        assert result is expected_df
        assert "falling back to pandas" in caplog.text.lower()


# ---------------------------------------------------------------------------
# Deferred imports
# ---------------------------------------------------------------------------


class TestDeferredImports:
    def test_module_import_does_not_pull_spark4_symbols(self) -> None:
        """Importing arrow_converter must not import pyspark.sql.pandas.types at module level."""
        # Reload the module and check that from_arrow_schema is not in its namespace
        import rivet_pyspark.arrow_converter as mod

        # The module should not have from_arrow_schema as a top-level name
        assert not hasattr(mod, "from_arrow_schema"), (
            "from_arrow_schema should be a deferred import, not at module level"
        )
        # Also verify pyspark.sql.pandas.types is not eagerly imported
        # (it may be in sys.modules from other tests, so we just check the module attrs)
        source = sys.modules[mod.__name__]
        top_level_names = [n for n in dir(source) if not n.startswith("_")]
        assert "from_arrow_schema" not in top_level_names
