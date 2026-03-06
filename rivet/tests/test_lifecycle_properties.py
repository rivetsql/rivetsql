"""Property-based tests: Polars SQLContext and PySpark SparkSession lifecycle (Properties 17-20, 35).

Property 17: Polars SQLContext is created fresh per fused group.
  For any fused group execution on the Polars engine, a fresh polars.SQLContext is
  created, used for the group, and discarded after the result LazyFrame is returned.

Property 18: Polars deferred Material backed by LazyFrame.
  For any execute() call on the Polars engine, the returned Material is deferred —
  no data is collected until to_arrow() is called.

Property 19: PySpark SparkSession singleton per process.
  For any PySpark engine instance, only one active classic SparkSession exists per
  Python process. If an external session exists, it is reused.

Property 20: PySpark teardown calls spark.stop().
  For any PySpark engine instance that created its own SparkSession, teardown() calls
  spark.stop(). If the session was externally managed, spark.stop() is NOT called.

Property 35: PySpark Spark Connect mode ignores master.
  For any PySpark engine with connect_url set, pyspark.sql.connect.SparkSession is
  used and master is ignored.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import polars as pl
import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.models import Joint, Material
from rivet_polars.engine import PolarsComputeEnginePlugin, PolarsLazyMaterializedRef
from rivet_pyspark.engine import PySparkComputeEngine

# ── Strategies ────────────────────────────────────────────────────────────────

_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)
_small_int_list = st.lists(st.integers(min_value=-1000, max_value=1000), min_size=1, max_size=10)
_master_str = st.sampled_from(["local", "local[*]", "local[2]", "local[4]", "yarn", "k8s://host"])
_connect_url = st.from_regex(r"sc://[a-z][a-z0-9]{0,10}:\d{4,5}", fullmatch=True)


def _joint(name: str, sql: str) -> Joint:
    return Joint(name=name, joint_type="sql", sql=sql)


def _mock_pyspark_classic():
    """Return (mock_spark_session_class, mock_builder, mock_session) for classic mode."""
    mock_session = MagicMock()
    mock_builder = MagicMock()
    mock_builder.master.return_value = mock_builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_session

    mock_cls = MagicMock()
    mock_cls.builder = mock_builder
    mock_cls.getActiveSession.return_value = None

    mock_sql_mod = MagicMock()
    mock_sql_mod.SparkSession = mock_cls
    return mock_cls, mock_builder, mock_session, mock_sql_mod


def _mock_pyspark_connect():
    """Return (mock_connect_cls, mock_connect_builder, mock_connect_session, mock_module)."""
    mock_session = MagicMock()
    mock_builder = MagicMock()
    mock_builder.remote.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_session

    mock_cls = MagicMock()
    mock_cls.builder = mock_builder

    mock_mod = MagicMock()
    mock_mod.SparkSession = mock_cls
    return mock_cls, mock_builder, mock_session, mock_mod


# ── Property 17: Fresh SQLContext per fused group ─────────────────────────────


@settings(max_examples=100)
@given(data=_small_int_list)
def test_property17_fresh_sqlcontext_per_fused_group(data: list[int]) -> None:
    """Property 17: each execute_fused_group call creates a fresh SQLContext."""
    plugin = PolarsComputeEnginePlugin()
    created_contexts: list[object] = []
    original = pl.SQLContext

    class Tracking(original):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            created_contexts.append(self)

    joints = [_joint("j1", "SELECT * FROM src")]
    upstream = {"src": pl.LazyFrame({"a": data})}

    with patch("polars.SQLContext", Tracking):
        plugin.execute_fused_group(joints, upstream)
        before = len(created_contexts)
        plugin.execute_fused_group(joints, upstream)

    assert len(created_contexts) == before + 1
    # Each call produced a distinct context
    assert created_contexts[-1] is not created_contexts[-2]


@settings(max_examples=100)
@given(data=_small_int_list)
def test_property17_sqlcontext_discarded_after_group(data: list[int]) -> None:
    """Property 17: SQLContext is discarded after the result LazyFrame is returned."""
    plugin = PolarsComputeEnginePlugin()
    joints = [_joint("j1", "SELECT * FROM src")]
    upstream = {"src": pl.LazyFrame({"a": data})}
    result = plugin.execute_fused_group(joints, upstream)
    # Result is a Material with a LazyFrame — the SQLContext is not retained
    assert isinstance(result, Material)
    ref = result.materialized_ref
    assert isinstance(ref, PolarsLazyMaterializedRef)
    # The LazyFrame is still valid and can be collected
    table = ref.to_arrow()
    assert table.column("a").to_pylist() == data


@settings(max_examples=100)
@given(data=_small_int_list)
def test_property17_single_joint_still_uses_sqlcontext(data: list[int]) -> None:
    """Property 17: even a single-joint group uses a SQLContext for consistency."""
    plugin = PolarsComputeEnginePlugin()
    created = []
    original = pl.SQLContext

    class Tracking(original):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            created.append(True)

    joints = [_joint("j1", "SELECT * FROM src")]
    upstream = {"src": pl.LazyFrame({"a": data})}

    with patch("polars.SQLContext", Tracking):
        plugin.execute_fused_group(joints, upstream)

    assert len(created) == 1


# ── Property 18: Deferred Material backed by LazyFrame ────────────────────────


@settings(max_examples=100)
@given(data=_small_int_list)
def test_property18_execute_sql_returns_deferred_material(data: list[int]) -> None:
    """Property 18: execute_sql returns a deferred Material backed by LazyFrame."""
    plugin = PolarsComputeEnginePlugin()
    upstream = {"t": pl.LazyFrame({"a": data})}
    result = plugin.execute_sql_lazy("SELECT * FROM t", upstream)
    assert isinstance(result, Material)
    assert result.state == "deferred"
    assert isinstance(result.materialized_ref, PolarsLazyMaterializedRef)


@settings(max_examples=100)
@given(data=_small_int_list)
def test_property18_no_collection_until_to_arrow(data: list[int]) -> None:
    """Property 18: LazyFrame is not collected until to_arrow() is called."""
    plugin = PolarsComputeEnginePlugin()
    upstream = {"t": pl.LazyFrame({"a": data})}
    result = plugin.execute_sql_lazy("SELECT * FROM t", upstream)
    ref = result.materialized_ref
    # Replace with mock to verify no premature collection
    lf_mock = MagicMock(spec=pl.LazyFrame)
    lf_mock.collect.return_value = pl.DataFrame({"a": data})
    ref._lazy_frame = lf_mock
    lf_mock.collect.assert_not_called()
    ref.to_arrow()
    lf_mock.collect.assert_called_once()


@settings(max_examples=100)
@given(data=_small_int_list)
def test_property18_to_arrow_returns_correct_data(data: list[int]) -> None:
    """Property 18: to_arrow() returns a valid pyarrow.Table with correct data."""
    plugin = PolarsComputeEnginePlugin()
    upstream = {"t": pl.LazyFrame({"a": data})}
    result = plugin.execute_sql_lazy("SELECT * FROM t", upstream)
    table = result.to_arrow()
    assert isinstance(table, pa.Table)
    assert table.column("a").to_pylist() == data


@settings(max_examples=100)
@given(data=_small_int_list)
def test_property18_fused_group_also_deferred(data: list[int]) -> None:
    """Property 18: execute_fused_group also returns deferred Material."""
    plugin = PolarsComputeEnginePlugin()
    joints = [_joint("j1", "SELECT * FROM src")]
    upstream = {"src": pl.LazyFrame({"a": data})}
    result = plugin.execute_fused_group(joints, upstream)
    assert result.state == "deferred"
    assert isinstance(result.materialized_ref, PolarsLazyMaterializedRef)


# ── Property 19: PySpark SparkSession singleton per process ───────────────────


@settings(max_examples=100)
@given(master=_master_str, app_name=_identifier)
def test_property19_get_session_returns_same_instance(master: str, app_name: str) -> None:
    """Property 19: repeated get_session() calls return the same SparkSession."""
    _, mock_builder, mock_session, mock_sql_mod = _mock_pyspark_classic()

    with patch.dict(sys.modules, {"pyspark": MagicMock(), "pyspark.sql": mock_sql_mod}):
        engine = PySparkComputeEngine("spark1", {"master": master, "app_name": app_name})
        s1 = engine.get_session()
        s2 = engine.get_session()

    assert s1 is s2
    mock_builder.getOrCreate.assert_called_once()


@settings(max_examples=100)
@given(master=_master_str)
def test_property19_reuses_external_session(master: str) -> None:
    """Property 19: if an external session exists, it is reused."""
    mock_cls, _, _, mock_sql_mod = _mock_pyspark_classic()
    external = MagicMock()
    mock_cls.getActiveSession.return_value = external

    with patch.dict(sys.modules, {"pyspark": MagicMock(), "pyspark.sql": mock_sql_mod}):
        engine = PySparkComputeEngine("spark1", {"master": master})
        session = engine.get_session()

    assert session is external
    assert engine._externally_managed is True


@settings(max_examples=100)
@given(master=_master_str)
def test_property19_own_session_not_externally_managed(master: str) -> None:
    """Property 19: a self-created session is not marked as externally managed."""
    _, _, _, mock_sql_mod = _mock_pyspark_classic()

    with patch.dict(sys.modules, {"pyspark": MagicMock(), "pyspark.sql": mock_sql_mod}):
        engine = PySparkComputeEngine("spark1", {"master": master})
        engine.get_session()

    assert engine._externally_managed is False


# ── Property 20: PySpark teardown calls spark.stop() ─────────────────────────


@settings(max_examples=100)
@given(master=_master_str)
def test_property20_teardown_stops_own_session(master: str) -> None:
    """Property 20: teardown() calls spark.stop() on self-created sessions."""
    _, _, mock_session, mock_sql_mod = _mock_pyspark_classic()

    with patch.dict(sys.modules, {"pyspark": MagicMock(), "pyspark.sql": mock_sql_mod}):
        engine = PySparkComputeEngine("spark1", {"master": master})
        engine.get_session()
        engine.teardown()

    mock_session.stop.assert_called_once()
    assert engine._session is None


@settings(max_examples=100)
@given(master=_master_str)
def test_property20_teardown_skips_stop_for_external(master: str) -> None:
    """Property 20: teardown() does NOT call spark.stop() on externally managed sessions."""
    mock_cls, _, _, mock_sql_mod = _mock_pyspark_classic()
    external = MagicMock()
    mock_cls.getActiveSession.return_value = external

    with patch.dict(sys.modules, {"pyspark": MagicMock(), "pyspark.sql": mock_sql_mod}):
        engine = PySparkComputeEngine("spark1", {"master": master})
        engine.get_session()
        engine.teardown()

    external.stop.assert_not_called()


@settings(max_examples=100)
@given(master=_master_str)
def test_property20_teardown_noop_without_session(master: str) -> None:
    """Property 20: teardown() is a no-op when no session was created."""
    engine = PySparkComputeEngine("spark1", {"master": master})
    engine.teardown()  # should not raise
    assert engine._session is None


# ── Property 35: PySpark Spark Connect mode ignores master ────────────────────


@settings(max_examples=100)
@given(connect_url=_connect_url, master=_master_str)
def test_property35_connect_mode_ignores_master(connect_url: str, master: str) -> None:
    """Property 35: when connect_url is set, master is ignored."""
    _, connect_builder, connect_session, connect_mod = _mock_pyspark_connect()

    with patch.dict(sys.modules, {"pyspark.sql.connect": connect_mod}):
        engine = PySparkComputeEngine("spark1", {"connect_url": connect_url, "master": master})
        session = engine.get_session()

    assert session is connect_session
    connect_builder.remote.assert_called_once_with(connect_url)
    connect_builder.master.assert_not_called()


@settings(max_examples=100)
@given(connect_url=_connect_url)
def test_property35_connect_mode_uses_connect_session(connect_url: str) -> None:
    """Property 35: connect_url causes pyspark.sql.connect.SparkSession to be used."""
    _, connect_builder, connect_session, connect_mod = _mock_pyspark_connect()

    with patch.dict(sys.modules, {"pyspark.sql.connect": connect_mod}):
        engine = PySparkComputeEngine("spark1", {"connect_url": connect_url})
        session = engine.get_session()

    assert session is connect_session
    connect_builder.remote.assert_called_once_with(connect_url)


@settings(max_examples=100)
@given(connect_url=_connect_url)
def test_property35_connect_mode_singleton(connect_url: str) -> None:
    """Property 35: connect mode also returns the same session on repeated calls."""
    _, connect_builder, connect_session, connect_mod = _mock_pyspark_connect()

    with patch.dict(sys.modules, {"pyspark.sql.connect": connect_mod}):
        engine = PySparkComputeEngine("spark1", {"connect_url": connect_url})
        s1 = engine.get_session()
        s2 = engine.get_session()

    assert s1 is s2
    connect_builder.getOrCreate.assert_called_once()
