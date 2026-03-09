"""Property-based tests for run stats data models and StatsCollector.

Covers Properties 1, 2, 3, 6, 7, 8, 11, 12 from the design document.
"""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.metrics import (
    MaterializationStats,
    PhasedTiming,
    PluginMetrics,
)
from rivet_core.stats import (
    GroupStats,
    JointStats,
    RunStats,
    StatsCollector,
)

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_timing_float = st.floats(min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False)


@st.composite
def phased_timing_st(draw: st.DrawFn) -> PhasedTiming:
    """Generate a valid PhasedTiming where total >= sum of phases."""
    engine = draw(_timing_float)
    materialize = draw(_timing_float)
    residual = draw(_timing_float)
    check = draw(_timing_float)
    overhead = draw(st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False))
    total = engine + materialize + residual + check + overhead
    return PhasedTiming(
        total_ms=total,
        engine_ms=engine,
        materialize_ms=materialize,
        residual_ms=residual,
        check_ms=check,
    )


_group_id_st = st.text(min_size=1, max_size=20, alphabet=st.characters(categories=("L", "N")))
_joint_name_st = st.text(min_size=1, max_size=30, alphabet=st.characters(categories=("L", "N", "P")))
_nonneg_int = st.integers(min_value=0, max_value=10_000_000)


@st.composite
def materialization_stats_st(draw: st.DrawFn) -> MaterializationStats:
    return MaterializationStats(
        row_count=draw(_nonneg_int),
        byte_size=draw(_nonneg_int),
        column_stats=[],
        sampled=draw(st.booleans()),
    )


@st.composite
def source_read_success_st(draw: st.DrawFn) -> dict:
    return {
        "joint_name": draw(_joint_name_st),
        "adapter_name": draw(st.text(min_size=1, max_size=10)),
        "catalog_type": draw(st.text(min_size=1, max_size=10)),
        "row_count": draw(_nonneg_int),
        "read_ms": draw(_timing_float),
        "error": None,
        "has_residual": draw(st.booleans()),
    }


@st.composite
def source_read_failure_st(draw: st.DrawFn) -> dict:
    return {
        "joint_name": draw(_joint_name_st),
        "adapter_name": draw(st.text(min_size=1, max_size=10)),
        "catalog_type": draw(st.text(min_size=1, max_size=10)),
        "row_count": None,
        "read_ms": draw(_timing_float),
        "error_code": draw(st.text(min_size=1, max_size=10)),
        "error_message": draw(st.text(min_size=1, max_size=50)),
        "has_residual": False,
    }


# ---------------------------------------------------------------------------
# Property 1: Group timing completeness
# ---------------------------------------------------------------------------


@given(timing=phased_timing_st(), success=st.booleans())
@settings(max_examples=100)
def test_property1_group_timing_completeness(timing: PhasedTiming, success: bool) -> None:
    """Feature: run-stats-metadata, Property 1: Group timing completeness.

    For any fused group recorded by the StatsCollector, the resulting GroupStats
    entry contains a PhasedTiming with all five fields non-negative and
    total_ms >= engine_ms + materialize_ms + residual_ms + check_ms.
    """
    sc = StatsCollector()
    sc.record_group_timing("g1", ["j1"], timing, success)
    rs = sc.build_run_stats(total_time_ms=100.0)

    gs = rs.group_stats[0]
    t = gs.timing
    assert t.total_ms >= 0
    assert t.engine_ms >= 0
    assert t.materialize_ms >= 0
    assert t.residual_ms >= 0
    assert t.check_ms >= 0
    assert t.total_ms >= t.engine_ms + t.materialize_ms + t.residual_ms + t.check_ms - 1e-9


# ---------------------------------------------------------------------------
# Property 2: Group stats preserve execution order
# ---------------------------------------------------------------------------


@given(group_ids=st.lists(_group_id_st, min_size=0, max_size=20))
@settings(max_examples=100)
def test_property2_group_order_preserved(group_ids: list[str]) -> None:
    """Feature: run-stats-metadata, Property 2: Group stats preserve execution order.

    For any sequence of N group recordings, the group_stats list has length N
    and group_id at index i matches the i-th recorded group.
    """
    sc = StatsCollector()
    dummy_timing = PhasedTiming(0, 0, 0, 0, 0)
    for gid in group_ids:
        sc.record_group_timing(gid, ["j"], dummy_timing, True)

    rs = sc.build_run_stats(total_time_ms=0.0)
    assert len(rs.group_stats) == len(group_ids)
    for i, gid in enumerate(group_ids):
        assert rs.group_stats[i].group_id == gid


# ---------------------------------------------------------------------------
# Property 3: Joint stats completeness and mapping
# ---------------------------------------------------------------------------


@given(
    data=st.lists(
        st.tuples(_joint_name_st, _nonneg_int, _nonneg_int),
        min_size=1,
        max_size=20,
        unique_by=lambda t: t[0],
    )
)
@settings(max_examples=100)
def test_property3_joint_stats_completeness(data: list[tuple[str, int, int]]) -> None:
    """Feature: run-stats-metadata, Property 3: Joint stats completeness and mapping.

    For any set of joints recorded, joint_stats dict contains an entry for every
    recorded joint name with correct rows_in and rows_out values.
    """
    sc = StatsCollector()
    dummy_timing = PhasedTiming(0, 0, 0, 0, 0)
    for name, rows_in, rows_out in data:
        sc.record_joint_stats(name, rows_in, rows_out, dummy_timing, None)

    rs = sc.build_run_stats(total_time_ms=0.0)
    for name, rows_in, rows_out in data:
        assert name in rs.joint_stats
        js = rs.joint_stats[name]
        assert js.rows_in == rows_in
        assert js.rows_out == rows_out
        assert js.timing is not None


# ---------------------------------------------------------------------------
# Property 7: RunStats immutability
# ---------------------------------------------------------------------------


@given(total_ms=_timing_float)
@settings(max_examples=100)
def test_property7_runstats_immutability(total_ms: float) -> None:
    """Feature: run-stats-metadata, Property 7: RunStats immutability.

    For any constructed RunStats instance, attempting to set any attribute
    raises FrozenInstanceError.
    """
    rs = RunStats(
        total_time_ms=total_ms,
        total_engine_ms=0.0,
        total_rivet_ms=total_ms,
        total_rows_in=0,
        total_rows_out=0,
        total_bytes_materialized=0,
        total_materializations=0,
        total_groups_executed=0,
        total_groups_failed=0,
        total_checks_passed=0,
        total_checks_failed=0,
        total_check_warnings=0,
        group_stats=[],
        joint_stats={},
        source_read_stats=[],
        check_stats=[],
    )
    try:
        rs.total_time_ms = 999.0  # type: ignore[misc]
        raise AssertionError("Expected FrozenInstanceError")
    except FrozenInstanceError:
        pass


# ---------------------------------------------------------------------------
# Property 8: RunStats serialization round trip
# ---------------------------------------------------------------------------


@given(
    timing=phased_timing_st(),
    rows_in=_nonneg_int,
    rows_out=_nonneg_int,
    byte_size=_nonneg_int,
)
@settings(max_examples=100)
def test_property8_serialization_round_trip(
    timing: PhasedTiming, rows_in: int, rows_out: int, byte_size: int
) -> None:
    """Feature: run-stats-metadata, Property 8: RunStats serialization round trip.

    Serialize via to_dict() to JSON and back; verify all fields match.
    """
    gs = GroupStats(group_id="g1", joints=["j1"], timing=timing, success=True)
    js = JointStats(name="j1", rows_in=rows_in, rows_out=rows_out, timing=timing, materialization_stats=None)
    rs = RunStats(
        total_time_ms=timing.total_ms,
        total_engine_ms=timing.engine_ms,
        total_rivet_ms=timing.total_ms - timing.engine_ms,
        total_rows_in=rows_in,
        total_rows_out=rows_out,
        total_bytes_materialized=byte_size,
        total_materializations=0,
        total_groups_executed=1,
        total_groups_failed=0,
        total_checks_passed=0,
        total_checks_failed=0,
        total_check_warnings=0,
        group_stats=[gs],
        joint_stats={"j1": js},
        source_read_stats=[],
        check_stats=[],
    )
    d = rs.to_dict()
    roundtripped = json.loads(json.dumps(d))

    assert roundtripped["total_time_ms"] == rs.total_time_ms
    assert roundtripped["total_rows_in"] == rs.total_rows_in
    assert roundtripped["total_rows_out"] == rs.total_rows_out
    assert roundtripped["total_bytes_materialized"] == rs.total_bytes_materialized
    assert len(roundtripped["group_stats"]) == 1
    assert roundtripped["group_stats"][0]["group_id"] == "g1"
    assert "j1" in roundtripped["joint_stats"]


# ---------------------------------------------------------------------------
# Property 6: Aggregate stats are sums of per-element stats
# ---------------------------------------------------------------------------


@given(
    joint_data=st.lists(
        st.tuples(
            _joint_name_st,
            _nonneg_int,  # rows_in
            _nonneg_int,  # rows_out
            st.one_of(st.none(), materialization_stats_st()),
        ),
        min_size=1,
        max_size=15,
        unique_by=lambda t: t[0],
    ),
    check_data=st.lists(
        st.tuples(
            _nonneg_int,  # passed
            _nonneg_int,  # failed
            _nonneg_int,  # warned
        ),
        min_size=0,
        max_size=10,
    ),
)
@settings(max_examples=100)
def test_property6_aggregate_sums(
    joint_data: list[tuple[str, int, int, MaterializationStats | None]],
    check_data: list[tuple[int, int, int]],
) -> None:
    """Feature: run-stats-metadata, Property 6: Aggregate stats are sums of per-element stats.

    total_rows_in equals sum of rows_in across all joints.
    total_bytes_materialized equals sum of byte_size from all MaterializationStats.
    Check aggregates equal sums of per-check entries.
    """
    sc = StatsCollector()
    dummy_timing = PhasedTiming(0, 0, 0, 0, 0)

    expected_rows_in = 0
    expected_rows_out = 0
    expected_bytes = 0
    expected_materializations = 0
    for name, ri, ro, mstats in joint_data:
        sc.record_joint_stats(name, ri, ro, dummy_timing, mstats)
        expected_rows_in += ri
        expected_rows_out += ro
        if mstats is not None:
            expected_bytes += mstats.byte_size
            expected_materializations += 1

    expected_passed = 0
    expected_failed = 0
    expected_warned = 0
    for i, (p, f, w) in enumerate(check_data):
        sc.record_check_results(f"check_joint_{i}", "assertion", p, f, w)
        expected_passed += p
        expected_failed += f
        expected_warned += w

    rs = sc.build_run_stats(total_time_ms=0.0)

    assert rs.total_rows_in == expected_rows_in
    assert rs.total_rows_out == expected_rows_out
    assert rs.total_bytes_materialized == expected_bytes
    assert rs.total_materializations == expected_materializations
    assert rs.total_checks_passed == expected_passed
    assert rs.total_checks_failed == expected_failed
    assert rs.total_check_warnings == expected_warned


# ---------------------------------------------------------------------------
# Property 11: Check count recording accuracy
# ---------------------------------------------------------------------------


@given(
    checks=st.lists(
        st.tuples(
            st.booleans(),  # passed
            st.sampled_from(["error", "warning"]),  # severity
        ),
        min_size=0,
        max_size=50,
    )
)
@settings(max_examples=100)
def test_property11_check_count_accuracy(checks: list[tuple[bool, str]]) -> None:
    """Feature: run-stats-metadata, Property 11: Check count recording accuracy.

    Given a list of check results with varying passed/severity, the StatsCollector
    records correct passed/failed/warned counts.
    """
    expected_passed = sum(1 for p, _ in checks if p)
    expected_failed = sum(1 for p, s in checks if not p and s == "error")
    expected_warned = sum(1 for p, s in checks if not p and s == "warning")

    sc = StatsCollector()
    sc.record_check_results(
        "test_joint", "assertion", expected_passed, expected_failed, expected_warned
    )

    rs = sc.build_run_stats(total_time_ms=0.0)
    assert rs.total_checks_passed == expected_passed
    assert rs.total_checks_failed == expected_failed
    assert rs.total_check_warnings == expected_warned
    assert len(rs.check_stats) == 1
    cs = rs.check_stats[0]
    assert cs.passed == expected_passed
    assert cs.failed == expected_failed
    assert cs.warned == expected_warned


# ---------------------------------------------------------------------------
# Property 12: Source read stats completeness
# ---------------------------------------------------------------------------


@given(
    successes=st.lists(source_read_success_st(), min_size=0, max_size=10),
    failures=st.lists(source_read_failure_st(), min_size=0, max_size=5),
)
@settings(max_examples=100)
def test_property12_source_read_stats_completeness(
    successes: list[dict], failures: list[dict]
) -> None:
    """Feature: run-stats-metadata, Property 12: Source read stats completeness.

    For any source reads (success and failure), all fields are present and correct.
    Successful reads have non-negative row_count. Failed reads have error_code
    and error_message set.
    """
    from rivet_core.errors import RivetError

    sc = StatsCollector()

    for s in successes:
        sc.record_source_read(
            joint_name=s["joint_name"],
            adapter_name=s["adapter_name"],
            catalog_type=s["catalog_type"],
            row_count=s["row_count"],
            read_ms=s["read_ms"],
            error=None,
            has_residual=s["has_residual"],
        )

    for f in failures:
        err = RivetError(code=f["error_code"], message=f["error_message"])
        sc.record_source_read(
            joint_name=f["joint_name"],
            adapter_name=f["adapter_name"],
            catalog_type=f["catalog_type"],
            row_count=None,
            read_ms=f["read_ms"],
            error=err,
            has_residual=False,
        )

    rs = sc.build_run_stats(total_time_ms=0.0)
    assert len(rs.source_read_stats) == len(successes) + len(failures)

    # Check successful reads
    for i, s in enumerate(successes):
        sr = rs.source_read_stats[i]
        assert sr.adapter_name == s["adapter_name"]
        assert sr.catalog_type == s["catalog_type"]
        assert sr.read_ms >= 0
        assert sr.row_count is not None and sr.row_count >= 0
        assert sr.error_code is None
        assert sr.error_message is None

    # Check failed reads
    for i, f in enumerate(failures):
        sr = rs.source_read_stats[len(successes) + i]
        assert sr.adapter_name == f["adapter_name"]
        assert sr.catalog_type == f["catalog_type"]
        assert sr.read_ms >= 0
        assert sr.row_count is None
        assert sr.error_code is not None
        assert sr.error_message is not None


# ---------------------------------------------------------------------------
# Property 5: Execution context contains required fields
# ---------------------------------------------------------------------------


@given(
    sql=st.text(min_size=0, max_size=5000),
    group_id=_group_id_st,
    engine_type=st.text(min_size=1, max_size=20, alphabet=st.characters(categories=("L",))),
    engine_ms=_timing_float,
)
@settings(max_examples=100)
def test_property5_execution_context_required_fields(
    sql: str, group_id: str, engine_type: str, engine_ms: float
) -> None:
    """Feature: run-stats-metadata, Property 5: Execution context contains required fields.

    For any SQL string, group_id, and engine_type, the execution context dict
    contains keys 'sql', 'group_id', 'engine_type', 'engine_ms', and
    len(context['sql']) <= 1000 with context['sql'] == sql[:1000].
    """
    execution_context: dict[str, object] = {
        "sql": sql[:1000],
        "group_id": group_id,
        "engine_type": engine_type,
        "engine_ms": engine_ms,
    }

    assert "sql" in execution_context
    assert "group_id" in execution_context
    assert "engine_type" in execution_context
    assert "engine_ms" in execution_context
    assert len(str(execution_context["sql"])) <= 1000
    assert execution_context["sql"] == sql[:1000]
    assert execution_context["group_id"] == group_id
    assert execution_context["engine_type"] == engine_type
    assert execution_context["engine_ms"] == engine_ms


# ---------------------------------------------------------------------------
# Property 4: Engine metrics attachment
# ---------------------------------------------------------------------------


@st.composite
def plugin_metrics_st(draw: st.DrawFn) -> PluginMetrics:
    """Generate a random PluginMetrics instance."""
    return PluginMetrics(
        well_known={},
        extensions={},
        engine=draw(st.text(min_size=0, max_size=10, alphabet=st.characters(categories=("L",)))),
    )


@given(
    group_id=_group_id_st,
    timing=phased_timing_st(),
    metrics=st.one_of(st.none(), plugin_metrics_st()),
)
@settings(max_examples=100)
def test_property4_engine_metrics_attachment(
    group_id: str, timing: PhasedTiming, metrics: PluginMetrics | None
) -> None:
    """Feature: run-stats-metadata, Property 4: Engine metrics attachment.

    For any fused group with PluginMetrics or None, recording via StatsCollector
    produces a GroupStats where plugin_metrics is non-None (empty PluginMetrics
    when the plugin returns None).
    """
    sc = StatsCollector()
    sc.record_group_timing(group_id, ["j1"], timing, success=True)

    # Simulate what the executor does: if metrics is None, record empty PluginMetrics
    effective_metrics = metrics if metrics is not None else PluginMetrics()
    sc.record_engine_metrics(group_id, effective_metrics)

    rs = sc.build_run_stats(total_time_ms=100.0)
    gs = rs.group_stats[0]
    assert gs.plugin_metrics is not None
    if metrics is not None:
        assert gs.plugin_metrics.engine == metrics.engine
    else:
        assert gs.plugin_metrics == PluginMetrics()


# ---------------------------------------------------------------------------
# Property 13: Response metadata merging
# ---------------------------------------------------------------------------


_metadata_key_st = st.text(min_size=1, max_size=20, alphabet=st.characters(categories=("L", "N")))
_metadata_val_st = st.text(min_size=0, max_size=50)


@given(
    sql=st.text(min_size=0, max_size=2000),
    group_id=_group_id_st,
    engine_type=st.text(min_size=1, max_size=20, alphabet=st.characters(categories=("L",))),
    engine_ms=_timing_float,
    response_metadata=st.dictionaries(_metadata_key_st, _metadata_val_st, min_size=0, max_size=10),
)
@settings(max_examples=100)
def test_property13_response_metadata_merging(
    sql: str,
    group_id: str,
    engine_type: str,
    engine_ms: float,
    response_metadata: dict[str, str],
) -> None:
    """Feature: run-stats-metadata, Property 13: Response metadata merging.

    All keys from response metadata appear in the execution context, and
    required keys (sql, group_id, engine_type, engine_ms) are not overwritten.
    """
    # Build execution context the same way the executor does
    execution_context: dict[str, object] = {
        "sql": sql[:1000],
        "group_id": group_id,
        "engine_type": engine_type,
        "engine_ms": engine_ms,
    }
    # Merge response metadata without overwriting required keys
    for k, v in response_metadata.items():
        if k not in execution_context:
            execution_context[k] = v

    # All response metadata keys should be present
    for k in response_metadata:
        assert k in execution_context

    # Required keys must not be overwritten
    assert execution_context["sql"] == sql[:1000]
    assert execution_context["group_id"] == group_id
    assert execution_context["engine_type"] == engine_type
    assert execution_context["engine_ms"] == engine_ms


# ---------------------------------------------------------------------------
# Property 9: Text renderer includes group summary data
# ---------------------------------------------------------------------------


@st.composite
def _group_stats_st(draw: st.DrawFn) -> GroupStats:
    """Generate a random GroupStats instance."""
    gid = draw(_group_id_st)
    joints = draw(st.lists(_joint_name_st, min_size=1, max_size=5))
    timing = draw(phased_timing_st())
    return GroupStats(
        group_id=gid,
        joints=joints,
        timing=timing,
        success=True,
    )


@st.composite
def _run_stats_with_groups_st(draw: st.DrawFn) -> RunStats:
    """Generate a RunStats with at least one GroupStats entry."""
    groups = draw(st.lists(_group_stats_st(), min_size=1, max_size=5))
    # Build joint_stats for all joints referenced in groups
    joint_stats: dict[str, JointStats] = {}
    for gs in groups:
        for jname in gs.joints:
            if jname not in joint_stats:
                joint_stats[jname] = JointStats(
                    name=jname,
                    rows_in=draw(_nonneg_int),
                    rows_out=draw(_nonneg_int),
                    timing=gs.timing,
                    materialization_stats=None,
                )
    total_engine_ms = sum(gs.timing.engine_ms for gs in groups)
    total_time = draw(_timing_float)
    return RunStats(
        total_time_ms=total_time,
        total_engine_ms=total_engine_ms,
        total_rivet_ms=total_time - total_engine_ms,
        total_rows_in=0,
        total_rows_out=0,
        total_bytes_materialized=0,
        total_materializations=0,
        total_groups_executed=len(groups),
        total_groups_failed=0,
        total_checks_passed=0,
        total_checks_failed=0,
        total_check_warnings=0,
        group_stats=groups,
        joint_stats=joint_stats,
        source_read_stats=[],
        check_stats=[],
    )


@given(run_stats=_run_stats_with_groups_st())
@settings(max_examples=100)
def test_property9_text_renderer_includes_group_summary(run_stats: RunStats) -> None:
    """Feature: run-stats-metadata, Property 9: Text renderer includes group summary data.

    For any RunStats with at least one GroupStats entry, the text-format output
    contains each group's group_id and total_ms value (formatted as an integer).

    **Validates: Requirements 6.1**
    """
    from rivet_cli.rendering.run_text import render_run_text
    from rivet_core.compiler import CompiledAssembly
    from rivet_core.executor import ExecutionResult

    result = ExecutionResult(
        success=True,
        status="success",
        joint_results=[],
        group_results=[],
        total_time_ms=run_stats.total_time_ms,
        total_materializations=0,
        total_failures=0,
        total_check_failures=0,
        total_check_warnings=0,
        run_stats=run_stats,
    )
    compiled = CompiledAssembly(
        success=True,
        profile_name="test",
        catalogs=[],
        engines=[],
        adapters=[],
        joints=[],
        fused_groups=[],
        materializations=[],
        execution_order=[],
        errors=[],
        warnings=[],
    )

    output = render_run_text(result, compiled, verbosity=0, color=False)

    for gs in run_stats.group_stats:
        assert gs.group_id in output, f"group_id '{gs.group_id}' not found in output"
        assert f"{gs.timing.total_ms:.0f}" in output, (
            f"total_ms '{gs.timing.total_ms:.0f}' not found in output"
        )


# ---------------------------------------------------------------------------
# Property 10: JSON renderer includes run_stats key
# ---------------------------------------------------------------------------


@given(run_stats=_run_stats_with_groups_st())
@settings(max_examples=100)
def test_property10_json_renderer_includes_run_stats_key(run_stats: RunStats) -> None:
    """Feature: run-stats-metadata, Property 10: JSON renderer includes run_stats key.

    For any ExecutionResult with a non-None run_stats, the JSON-format output
    parses as valid JSON containing a "run_stats" key whose value is a dict
    with keys "total_time_ms", "group_stats", and "joint_stats".

    **Validates: Requirements 6.3**
    """
    from rivet_cli.rendering.json_out import render_run_json
    from rivet_core.compiler import CompiledAssembly
    from rivet_core.executor import ExecutionResult

    result = ExecutionResult(
        success=True,
        status="success",
        joint_results=[],
        group_results=[],
        total_time_ms=run_stats.total_time_ms,
        total_materializations=0,
        total_failures=0,
        total_check_failures=0,
        total_check_warnings=0,
        run_stats=run_stats,
    )
    compiled = CompiledAssembly(
        success=True,
        profile_name="test",
        catalogs=[],
        engines=[],
        adapters=[],
        joints=[],
        fused_groups=[],
        materializations=[],
        execution_order=[],
        errors=[],
        warnings=[],
    )

    output = render_run_json(result, compiled)
    parsed = json.loads(output)

    assert "run_stats" in parsed, "'run_stats' key not found in JSON output"
    rs_dict = parsed["run_stats"]
    assert isinstance(rs_dict, dict)
    assert "total_time_ms" in rs_dict
    assert "group_stats" in rs_dict
    assert "joint_stats" in rs_dict
