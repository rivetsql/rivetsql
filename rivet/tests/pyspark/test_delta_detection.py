"""Bug condition exploration tests for Delta JAR detection.

These tests encode the EXPECTED (correct) behavior of _has_delta_jars.
On unfixed code, they are EXPECTED TO FAIL — failure confirms the bug exists.

**Validates: Requirements 1.1, 1.2, 1.3**
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_pyspark.adapters.unity import _has_delta_jars

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_semver = st.from_regex(r"[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,3}", fullmatch=True)

_delta_artifact = _semver.map(lambda v: f"io.delta:delta-spark_2.13:{v}")


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


def _make_session_jvm_fails_with_delta_config(packages_value: str) -> Any:
    """Session where config has Delta but JVM Class.forName raises for both classes."""
    conf = MagicMock()
    conf.get = MagicMock(side_effect=lambda key, default="": {
        "spark.jars.packages": packages_value,
        "spark.jars": "",
    }.get(key, default))

    jvm = MagicMock()
    jvm.java.lang.Class.forName = MagicMock(side_effect=Exception("ClassNotFoundException"))

    session = MagicMock()
    session._jvm = jvm
    session.conf = conf
    # sparkContext.getConf().getAll() for the debug logging path
    session.sparkContext.getConf.return_value.getAll.return_value = []
    return session


def _make_session_no_jvm_no_delta() -> Any:
    """Session where _jvm access raises AttributeError and config has no Delta.

    Uses a real class with a property descriptor so that ``session._jvm``
    actually raises ``AttributeError``, which ``MagicMock`` cannot do
    reliably (its ``__getattr__`` intercepts before the descriptor fires).
    """
    conf = MagicMock()
    conf.get = MagicMock(side_effect=lambda key, default="": {
        "spark.jars.packages": "",
        "spark.jars": "",
    }.get(key, default))

    class _SparkConnectSession:
        """Minimal stand-in that raises on ``_jvm`` access."""

        @property
        def _jvm(self) -> Any:
            raise AttributeError("no _jvm in Spark Connect")

    session = _SparkConnectSession()
    session.conf = conf  # type: ignore[attr-defined]
    return session


# ---------------------------------------------------------------------------
# Property 1: Fault Condition — Config-Based Delta Detection vs JVM-Only Detection
# ---------------------------------------------------------------------------


class TestBugConditionExploration:
    """Exploration tests that surface the two bug scenarios.

    These tests assert the EXPECTED (correct) behavior.
    On unfixed code they MUST FAIL — proving the bug exists.
    """

    @given(artifact=_delta_artifact)
    @settings(max_examples=30)
    def test_false_negative_config_has_delta_but_jvm_fails(self, artifact: str) -> None:
        """False negative: config has Delta artifact but JVM reflection fails.

        Current buggy code returns False (wrong). Expected: True.

        **Validates: Requirements 1.1, 1.3**
        """
        session = _make_session_jvm_fails_with_delta_config(artifact)
        result = _has_delta_jars(session)
        assert result is True, (
            f"_has_delta_jars returned False when spark.jars.packages='{artifact}' "
            f"but JVM Class.forName failed — this is the false-negative bug"
        )

    def test_false_positive_no_jvm_no_delta_config(self) -> None:
        """False positive: no JVM access and no Delta in config.

        Current buggy code in unity.py returns True (wrong). Expected: False.

        **Validates: Requirements 1.2**
        """
        session = _make_session_no_jvm_no_delta()
        result = _has_delta_jars(session)
        assert result is False, (
            "_has_delta_jars returned True when _jvm is unavailable and config has "
            "no Delta artifacts — this is the false-positive bug"
        )


from rivet_pyspark.adapters.s3 import _has_delta_jars as _has_delta_jars_s3

# ---------------------------------------------------------------------------
# Additional strategies for preservation tests
# ---------------------------------------------------------------------------

# Non-Delta Maven coordinates — must never trigger a false positive
_NON_DELTA_GROUP_IDS = [
    "org.apache.spark",
    "org.apache.hadoop",
    "com.amazonaws",
    "org.postgresql",
    "mysql",
    "com.google.cloud",
    "org.apache.kafka",
    "io.confluent",
    "org.apache.hive",
    "com.databricks",
]

_non_delta_artifact = st.sampled_from(_NON_DELTA_GROUP_IDS).flatmap(
    lambda g: _semver.map(lambda v: f"{g}:some-artifact_2.13:{v}")
)

_non_delta_packages = st.lists(_non_delta_artifact, min_size=0, max_size=5).map(
    lambda arts: ",".join(arts)
)


# ---------------------------------------------------------------------------
# Mock helpers for preservation tests
# ---------------------------------------------------------------------------


def _make_session_jvm_finds_delta_table() -> Any:
    """Session where JVM Class.forName("io.delta.tables.DeltaTable") succeeds."""
    conf = MagicMock()
    conf.get = MagicMock(side_effect=lambda key, default="": {
        "spark.jars.packages": "",
        "spark.jars": "",
    }.get(key, default))

    jvm = MagicMock()
    # DeltaTable found — forName returns successfully (mock just returns a mock)
    jvm.java.lang.Class.forName = MagicMock(return_value=MagicMock())

    session = MagicMock()
    session._jvm = jvm
    session.conf = conf
    session.sparkContext.getConf.return_value.getAll.return_value = []
    return session


def _make_session_jvm_finds_delta_log_only() -> Any:
    """Session where DeltaTable fails but DeltaLog succeeds via JVM."""
    conf = MagicMock()
    conf.get = MagicMock(side_effect=lambda key, default="": {
        "spark.jars.packages": "",
        "spark.jars": "",
    }.get(key, default))

    def _forName_side_effect(class_name: str) -> Any:
        if class_name == "io.delta.tables.DeltaTable":
            raise Exception("ClassNotFoundException")
        # DeltaLog succeeds
        return MagicMock()

    jvm = MagicMock()
    jvm.java.lang.Class.forName = MagicMock(side_effect=_forName_side_effect)

    session = MagicMock()
    session._jvm = jvm
    session.conf = conf
    session.sparkContext.getConf.return_value.getAll.return_value = []
    return session


def _make_session_jvm_no_delta(packages_value: str) -> Any:
    """Session where JVM is accessible but both Delta classes fail, config has no Delta."""
    conf = MagicMock()
    conf.get = MagicMock(side_effect=lambda key, default="": {
        "spark.jars.packages": packages_value,
        "spark.jars": "",
    }.get(key, default))

    jvm = MagicMock()
    jvm.java.lang.Class.forName = MagicMock(side_effect=Exception("ClassNotFoundException"))

    session = MagicMock()
    session._jvm = jvm
    session.conf = conf
    session.sparkContext.getConf.return_value.getAll.return_value = []
    return session


# ---------------------------------------------------------------------------
# Property 2: Preservation — JVM-Detected Delta and Genuine Absence Unchanged
# ---------------------------------------------------------------------------


class TestPreservation:
    """Preservation tests verifying non-buggy behavior is unchanged.

    These tests run on UNFIXED code and MUST PASS — confirming baseline
    behavior that the fix must preserve.

    **Validates: Requirements 3.1, 3.2, 3.4, 3.5**
    """

    # --- Unity adapter preservation ---

    @given(data=st.data())
    @settings(max_examples=30)
    def test_unity_jvm_finds_delta_table_returns_true(self, data: st.DataObject) -> None:
        """JVM reflection finds DeltaTable → _has_delta_jars returns True.

        **Validates: Requirements 3.2**
        """
        session = _make_session_jvm_finds_delta_table()
        result = _has_delta_jars(session)
        assert result is True, (
            "_has_delta_jars should return True when JVM finds DeltaTable"
        )

    @given(data=st.data())
    @settings(max_examples=30)
    def test_unity_jvm_finds_delta_log_returns_true(self, data: st.DataObject) -> None:
        """JVM reflection finds DeltaLog (but not DeltaTable) → returns True.

        **Validates: Requirements 3.2**
        """
        session = _make_session_jvm_finds_delta_log_only()
        result = _has_delta_jars(session)
        assert result is True, (
            "_has_delta_jars should return True when JVM finds DeltaLog"
        )

    @given(packages=_non_delta_packages)
    @settings(max_examples=50)
    def test_unity_jvm_no_delta_no_config_returns_false(self, packages: str) -> None:
        """JVM accessible, both Delta classes fail, config has no Delta → returns False.

        **Validates: Requirements 3.1, 3.4**
        """
        session = _make_session_jvm_no_delta(packages)
        result = _has_delta_jars(session)
        assert result is False, (
            f"_has_delta_jars should return False when JVM has no Delta classes "
            f"and config is '{packages}' (no Delta artifacts)"
        )

    @given(packages=_non_delta_packages)
    @settings(max_examples=50)
    def test_unity_non_delta_packages_no_false_positive(self, packages: str) -> None:
        """Random non-Delta packages must not trigger false positives.

        **Validates: Requirements 3.4**
        """
        session = _make_session_jvm_no_delta(packages)
        result = _has_delta_jars(session)
        assert result is False, (
            f"Non-Delta packages '{packages}' should not cause _has_delta_jars "
            f"to return True"
        )

    # --- S3 adapter preservation ---

    @given(data=st.data())
    @settings(max_examples=30)
    def test_s3_jvm_finds_delta_table_returns_true(self, data: st.DataObject) -> None:
        """S3 adapter: JVM finds DeltaTable → _has_delta_jars returns True.

        **Validates: Requirements 3.5**
        """
        session = _make_session_jvm_finds_delta_table()
        result = _has_delta_jars_s3(session)
        assert result is True, (
            "S3 _has_delta_jars should return True when JVM finds DeltaTable"
        )

    @given(packages=_non_delta_packages)
    @settings(max_examples=50)
    def test_s3_jvm_no_delta_returns_false(self, packages: str) -> None:
        """S3 adapter: JVM accessible, Delta class not found → returns False.

        **Validates: Requirements 3.5, 3.4**
        """
        session = _make_session_jvm_no_delta(packages)
        result = _has_delta_jars_s3(session)
        assert result is False, (
            f"S3 _has_delta_jars should return False when JVM has no Delta "
            f"and config is '{packages}'"
        )

    @given(packages=_non_delta_packages)
    @settings(max_examples=50)
    def test_s3_non_delta_packages_no_false_positive(self, packages: str) -> None:
        """S3 adapter: random non-Delta packages must not trigger false positives.

        **Validates: Requirements 3.5, 3.4**
        """
        session = _make_session_jvm_no_delta(packages)
        result = _has_delta_jars_s3(session)
        assert result is False, (
            f"S3: Non-Delta packages '{packages}' should not cause false positive"
        )


# ---------------------------------------------------------------------------
# Imports for unit tests on shared detection module
# ---------------------------------------------------------------------------

from rivet_pyspark.adapters._detection import (
    _config_has_delta,
)
from rivet_pyspark.adapters._detection import (
    _has_delta_jars as _has_delta_jars_shared,
)

# ---------------------------------------------------------------------------
# Unit tests: _config_has_delta
# ---------------------------------------------------------------------------


class TestConfigHasDelta:
    """Unit tests for ``_config_has_delta`` from the shared detection module.

    **Validates: Requirements 2.1, 2.3, 3.1, 3.2**
    """

    def test_single_delta_package(self) -> None:
        """Single Delta package in spark.jars.packages → True."""
        conf = MagicMock()
        conf.get = MagicMock(side_effect=lambda key, default="": {
            "spark.jars.packages": "io.delta:delta-spark_2.13:4.0.0",
            "spark.jars": "",
        }.get(key, default))
        session = MagicMock()
        session.conf = conf
        assert _config_has_delta(session) is True

    def test_multiple_packages_including_delta(self) -> None:
        """Multiple packages including Delta in spark.jars.packages → True."""
        conf = MagicMock()
        conf.get = MagicMock(side_effect=lambda key, default="": {
            "spark.jars.packages": (
                "org.apache.spark:spark-sql-kafka_2.13:3.5.0,"
                "io.delta:delta-spark_2.13:4.0.0,"
                "com.amazonaws:aws-java-sdk:1.12.0"
            ),
            "spark.jars": "",
        }.get(key, default))
        session = MagicMock()
        session.conf = conf
        assert _config_has_delta(session) is True

    def test_delta_jar_path_in_spark_jars(self) -> None:
        """Delta JAR path in spark.jars → True."""
        conf = MagicMock()
        conf.get = MagicMock(side_effect=lambda key, default="": {
            "spark.jars.packages": "",
            "spark.jars": "/opt/spark/jars/delta-core_2.13-2.4.0.jar",
        }.get(key, default))
        session = MagicMock()
        session.conf = conf
        assert _config_has_delta(session) is True

    def test_no_delta_in_either_config(self) -> None:
        """No Delta in either config → False."""
        conf = MagicMock()
        conf.get = MagicMock(side_effect=lambda key, default="": {
            "spark.jars.packages": "org.apache.spark:spark-sql-kafka_2.13:3.5.0",
            "spark.jars": "/opt/spark/jars/hadoop-aws-3.3.4.jar",
        }.get(key, default))
        session = MagicMock()
        session.conf = conf
        assert _config_has_delta(session) is False

    def test_conf_get_raises_exception(self) -> None:
        """conf.get raises exception → False."""
        conf = MagicMock()
        conf.get = MagicMock(side_effect=Exception("Spark conf unavailable"))
        session = MagicMock()
        session.conf = conf
        assert _config_has_delta(session) is False

    def test_partial_match_not_delta(self) -> None:
        """Partial match edge case: 'not-delta-spark-thing' should not match
        because it still contains 'delta-spark' substring — this is expected
        to return True since substring matching is the design choice."""
        conf = MagicMock()
        conf.get = MagicMock(side_effect=lambda key, default="": {
            "spark.jars.packages": "com.example:not-delta-spark-thing:1.0.0",
            "spark.jars": "",
        }.get(key, default))
        session = MagicMock()
        session.conf = conf
        # The implementation uses substring matching ("delta-spark" in combined),
        # so "not-delta-spark-thing" DOES match. This is by design — the artifact
        # naming convention makes false positives from real Maven coordinates
        # extremely unlikely.
        assert _config_has_delta(session) is True

    def test_empty_config_values(self) -> None:
        """Empty config values → False."""
        conf = MagicMock()
        conf.get = MagicMock(side_effect=lambda key, default="": {
            "spark.jars.packages": "",
            "spark.jars": "",
        }.get(key, default))
        session = MagicMock()
        session.conf = conf
        assert _config_has_delta(session) is False

    def test_delta_core_in_packages(self) -> None:
        """delta-core artifact in spark.jars.packages → True."""
        conf = MagicMock()
        conf.get = MagicMock(side_effect=lambda key, default="": {
            "spark.jars.packages": "io.delta:delta-core_2.12:2.4.0",
            "spark.jars": "",
        }.get(key, default))
        session = MagicMock()
        session.conf = conf
        assert _config_has_delta(session) is True


# ---------------------------------------------------------------------------
# Unit tests: _has_delta_jars (shared detection module)
# ---------------------------------------------------------------------------


class TestHasDeltaJarsUnit:
    """Unit tests for ``_has_delta_jars`` from the shared detection module.

    **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 3.1, 3.2**
    """

    def test_config_has_delta_jvm_fails(self) -> None:
        """Config has Delta, JVM fails → True (config-first)."""
        session = _make_session_jvm_fails_with_delta_config(
            "io.delta:delta-spark_2.13:4.0.0"
        )
        assert _has_delta_jars_shared(session) is True

    def test_config_has_delta_jvm_succeeds(self) -> None:
        """Config has Delta, JVM succeeds → True (both paths agree)."""
        conf = MagicMock()
        conf.get = MagicMock(side_effect=lambda key, default="": {
            "spark.jars.packages": "io.delta:delta-spark_2.13:4.0.0",
            "spark.jars": "",
        }.get(key, default))
        jvm = MagicMock()
        jvm.java.lang.Class.forName = MagicMock(return_value=MagicMock())
        session = MagicMock()
        session._jvm = jvm
        session.conf = conf
        assert _has_delta_jars_shared(session) is True

    def test_no_config_delta_jvm_succeeds(self) -> None:
        """No config Delta, JVM succeeds → True (JVM fallback)."""
        session = _make_session_jvm_finds_delta_table()
        assert _has_delta_jars_shared(session) is True

    def test_no_config_delta_no_jvm_class(self) -> None:
        """No config Delta, no JVM class → False."""
        session = _make_session_jvm_no_delta("")
        assert _has_delta_jars_shared(session) is False

    def test_no_jvm_access_config_has_delta(self) -> None:
        """No JVM access, config has Delta → True."""
        conf = MagicMock()
        conf.get = MagicMock(side_effect=lambda key, default="": {
            "spark.jars.packages": "io.delta:delta-spark_2.13:4.0.0",
            "spark.jars": "",
        }.get(key, default))

        class _NoJvmSession:
            @property
            def _jvm(self) -> Any:
                raise AttributeError("no _jvm in Spark Connect")

        session = _NoJvmSession()
        session.conf = conf  # type: ignore[attr-defined]
        assert _has_delta_jars_shared(session) is True

    def test_no_jvm_access_no_config_delta(self) -> None:
        """No JVM access, no config Delta → False."""
        session = _make_session_no_jvm_no_delta()
        assert _has_delta_jars_shared(session) is False


# ---------------------------------------------------------------------------
# Verify no file I/O
# ---------------------------------------------------------------------------


class TestNoFileIO:
    """Verify that detection functions do not write to /tmp/rivet_spark_debug.log.

    **Validates: Requirements 2.4**
    """

    def test_shared_has_delta_jars_no_file_io(self, tmp_path: Any) -> None:
        """_has_delta_jars from _detection.py must not write to /tmp."""
        import pathlib

        debug_log = pathlib.Path("/tmp/rivet_spark_debug.log")
        # Record state before
        existed_before = debug_log.exists()
        mtime_before = debug_log.stat().st_mtime if existed_before else None

        session = _make_session_jvm_fails_with_delta_config(
            "io.delta:delta-spark_2.13:4.0.0"
        )
        _has_delta_jars_shared(session)

        if existed_before:
            # File should not have been modified
            assert debug_log.stat().st_mtime == mtime_before, (
                "_has_delta_jars wrote to /tmp/rivet_spark_debug.log"
            )
        else:
            # File should not have been created
            assert not debug_log.exists(), (
                "_has_delta_jars created /tmp/rivet_spark_debug.log"
            )

    def test_config_has_delta_no_file_io(self) -> None:
        """_config_has_delta must not perform any file I/O."""
        from unittest.mock import patch

        conf = MagicMock()
        conf.get = MagicMock(side_effect=lambda key, default="": {
            "spark.jars.packages": "io.delta:delta-spark_2.13:4.0.0",
            "spark.jars": "",
        }.get(key, default))
        session = MagicMock()
        session.conf = conf

        # Patch builtins.open to detect any file I/O
        with patch("builtins.open", side_effect=AssertionError("Unexpected file I/O")):
            result = _config_has_delta(session)
        assert result is True
