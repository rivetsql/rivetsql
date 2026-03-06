# Feature: databricks-unity-adapter, Property 1: Read SQL generation with time travel
"""Property-based tests for DatabricksUnityAdapter.

Property 1: Read SQL generation with time travel.
Property 2: Read dispatch returns valid AdapterPushdownResult.

Validates: Requirements 2.1, 2.2, 2.3, 2.4
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_databricks.adapters.unity import _build_read_sql

# ── Strategies ────────────────────────────────────────────────────────

# Name parts: non-empty identifiers without dots or whitespace
_name_part = st.from_regex(r"[a-zA-Z_][a-zA-Z0-9_]{0,30}", fullmatch=True)

_three_part_table = st.tuples(_name_part, _name_part, _name_part).map(
    lambda t: f"{t[0]}.{t[1]}.{t[2]}"
)

_version = st.one_of(st.none(), st.integers(min_value=0, max_value=2**31))

_iso_timestamp = st.from_regex(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", fullmatch=True
)
_timestamp = st.one_of(st.none(), _iso_timestamp)


def _make_deferred_ref(
    version: int | None, timestamp: str | None
) -> MagicMock:
    """Create a mock UnityDeferredMaterializedRef with time travel properties."""
    ref = MagicMock()
    ref.effective_version = version
    # Version takes precedence: if version is set, effective_timestamp is None
    ref.effective_timestamp = None if version is not None else timestamp
    return ref


# ── Property tests ────────────────────────────────────────────────────


@settings(max_examples=100)
@given(table=_three_part_table, version=_version, timestamp=_timestamp)
def test_read_sql_starts_with_select_from_table(
    table: str, version: int | None, timestamp: str | None
) -> None:
    """Generated SQL always starts with SELECT * FROM {table} (or column list)."""
    ref = _make_deferred_ref(version, timestamp)
    sql, _ = _build_read_sql(table, ref, None)
    assert sql.startswith(f"SELECT * FROM {table}")


@settings(max_examples=100)
@given(table=_three_part_table, version=st.integers(min_value=0, max_value=2**31))
def test_read_sql_version_clause(table: str, version: int) -> None:
    """When version is provided, SQL contains VERSION AS OF {version}."""
    ref = _make_deferred_ref(version, None)
    sql, _ = _build_read_sql(table, ref, None)
    assert f"VERSION AS OF {version}" in sql


@settings(max_examples=100)
@given(table=_three_part_table, timestamp=_iso_timestamp)
def test_read_sql_timestamp_clause_without_version(
    table: str, timestamp: str
) -> None:
    """When timestamp is provided and no version, SQL contains TIMESTAMP AS OF."""
    ref = _make_deferred_ref(None, timestamp)
    sql, _ = _build_read_sql(table, ref, None)
    assert f"TIMESTAMP AS OF '{timestamp}'" in sql
    assert "VERSION AS OF" not in sql


@settings(max_examples=100)
@given(table=_three_part_table)
def test_read_sql_no_time_travel_when_neither(table: str) -> None:
    """When neither version nor timestamp, SQL has no time travel clause."""
    ref = _make_deferred_ref(None, None)
    sql, _ = _build_read_sql(table, ref, None)
    assert "VERSION AS OF" not in sql
    assert "TIMESTAMP AS OF" not in sql
    assert sql == f"SELECT * FROM {table}"


@settings(max_examples=100)
@given(
    table=_three_part_table,
    version=st.integers(min_value=0, max_value=2**31),
    timestamp=_iso_timestamp,
)
def test_version_takes_precedence_over_timestamp(
    table: str, version: int, timestamp: str
) -> None:
    """When both version and timestamp are provided, version wins."""
    ref = _make_deferred_ref(version, timestamp)
    sql, _ = _build_read_sql(table, ref, None)
    assert f"VERSION AS OF {version}" in sql
    assert "TIMESTAMP AS OF" not in sql


@settings(max_examples=100)
@given(table=_three_part_table, version=_version, timestamp=_timestamp)
def test_read_sql_returns_empty_residual_without_pushdown(
    table: str, version: int | None, timestamp: str | None
) -> None:
    """Without pushdown, residual is EMPTY_RESIDUAL."""
    from rivet_core.optimizer import EMPTY_RESIDUAL

    ref = _make_deferred_ref(version, timestamp)
    _, residual = _build_read_sql(table, ref, None)
    assert residual == EMPTY_RESIDUAL


# ── Property 2: Read dispatch returns valid AdapterPushdownResult ─────
# Feature: databricks-unity-adapter, Property 2: Read dispatch returns valid AdapterPushdownResult
#
# For any valid engine config, catalog, and joint, read_dispatch must return an
# AdapterPushdownResult whose material has state="deferred", a MaterializedRef
# with storage_type="databricks", and a ResidualPlan.
#
# Validates: Requirements 2.2

_joint_name = st.from_regex(r"[a-z][a-z0-9_]{0,20}", fullmatch=True)
_catalog_name = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)


def _make_engine(workspace_url: str = "https://test.databricks.com", token: str = "tok", warehouse_id: str = "wh1") -> MagicMock:
    engine = MagicMock()
    engine.config = {"workspace_url": workspace_url, "token": token, "warehouse_id": warehouse_id}
    return engine


def _make_catalog(name: str, catalog_opt: str, schema_opt: str) -> MagicMock:
    cat = MagicMock()
    cat.name = name
    cat.options = {"catalog": catalog_opt, "schema": schema_opt}
    return cat


def _make_joint(name: str, table: str) -> MagicMock:
    joint = MagicMock()
    joint.name = name
    joint.table = table
    joint.joint_type = "source"
    joint.source_options = {}
    return joint


@settings(max_examples=100)
@given(
    joint_name=_joint_name,
    cat_name=_catalog_name,
    cat_opt=_catalog_name,
    schema_opt=_catalog_name,
)
@patch("rivet_databricks.engine.DatabricksStatementAPI")
def test_read_dispatch_returns_adapter_pushdown_result(
    mock_api_cls: MagicMock,
    joint_name: str,
    cat_name: str,
    cat_opt: str,
    schema_opt: str,
) -> None:
    """read_dispatch returns an AdapterPushdownResult."""
    from rivet_core.optimizer import AdapterPushdownResult
    from rivet_databricks.adapters.unity import DatabricksUnityAdapter

    table = f"{cat_opt}.{schema_opt}.{joint_name}"
    engine = _make_engine()
    catalog = _make_catalog(cat_name, cat_opt, schema_opt)
    joint = _make_joint(joint_name, table)

    adapter = DatabricksUnityAdapter()
    result = adapter.read_dispatch(engine, catalog, joint)

    assert isinstance(result, AdapterPushdownResult)


@settings(max_examples=100)
@given(
    joint_name=_joint_name,
    cat_name=_catalog_name,
    cat_opt=_catalog_name,
    schema_opt=_catalog_name,
)
@patch("rivet_databricks.engine.DatabricksStatementAPI")
def test_read_dispatch_material_state_is_deferred(
    mock_api_cls: MagicMock,
    joint_name: str,
    cat_name: str,
    cat_opt: str,
    schema_opt: str,
) -> None:
    """Returned material has state='deferred'."""
    from rivet_databricks.adapters.unity import DatabricksUnityAdapter

    table = f"{cat_opt}.{schema_opt}.{joint_name}"
    engine = _make_engine()
    catalog = _make_catalog(cat_name, cat_opt, schema_opt)
    joint = _make_joint(joint_name, table)

    result = DatabricksUnityAdapter().read_dispatch(engine, catalog, joint)

    assert result.material.state == "deferred"


@settings(max_examples=100)
@given(
    joint_name=_joint_name,
    cat_name=_catalog_name,
    cat_opt=_catalog_name,
    schema_opt=_catalog_name,
)
@patch("rivet_databricks.engine.DatabricksStatementAPI")
def test_read_dispatch_materialized_ref_storage_type(
    mock_api_cls: MagicMock,
    joint_name: str,
    cat_name: str,
    cat_opt: str,
    schema_opt: str,
) -> None:
    """Returned MaterializedRef has storage_type='databricks'."""
    from rivet_databricks.adapters.unity import DatabricksUnityAdapter

    table = f"{cat_opt}.{schema_opt}.{joint_name}"
    engine = _make_engine()
    catalog = _make_catalog(cat_name, cat_opt, schema_opt)
    joint = _make_joint(joint_name, table)

    result = DatabricksUnityAdapter().read_dispatch(engine, catalog, joint)

    assert result.material.materialized_ref is not None
    assert result.material.materialized_ref.storage_type == "databricks"


@settings(max_examples=100)
@given(
    joint_name=_joint_name,
    cat_name=_catalog_name,
    cat_opt=_catalog_name,
    schema_opt=_catalog_name,
)
@patch("rivet_databricks.engine.DatabricksStatementAPI")
def test_read_dispatch_has_residual_plan(
    mock_api_cls: MagicMock,
    joint_name: str,
    cat_name: str,
    cat_opt: str,
    schema_opt: str,
) -> None:
    """Returned result has a ResidualPlan."""

    from rivet_databricks.adapters.unity import DatabricksUnityAdapter

    table = f"{cat_opt}.{schema_opt}.{joint_name}"
    engine = _make_engine()
    catalog = _make_catalog(cat_name, cat_opt, schema_opt)
    joint = _make_joint(joint_name, table)

    result = DatabricksUnityAdapter().read_dispatch(engine, catalog, joint)

    assert isinstance(result.residual, ResidualPlan)


@settings(max_examples=100)
@given(
    joint_name=_joint_name,
    cat_name=_catalog_name,
    cat_opt=_catalog_name,
    schema_opt=_catalog_name,
)
@patch("rivet_databricks.engine.DatabricksStatementAPI")
def test_read_dispatch_material_name_matches_joint(
    mock_api_cls: MagicMock,
    joint_name: str,
    cat_name: str,
    cat_opt: str,
    schema_opt: str,
) -> None:
    """Returned material name matches the joint name."""
    from rivet_databricks.adapters.unity import DatabricksUnityAdapter

    table = f"{cat_opt}.{schema_opt}.{joint_name}"
    engine = _make_engine()
    catalog = _make_catalog(cat_name, cat_opt, schema_opt)
    joint = _make_joint(joint_name, table)

    result = DatabricksUnityAdapter().read_dispatch(engine, catalog, joint)

    assert result.material.name == joint_name
    assert result.material.catalog == cat_name


# ── Property 3: Pushdown SQL modification ─────────────────────────────
# Feature: databricks-unity-adapter, Property 3: Pushdown SQL modification
#
# For any valid base SELECT SQL and PushdownPlan with projections, filters,
# and/or limit, the generated SQL must incorporate pushed projections (replacing
# * with column list), pushed predicates (as WHERE clause), and pushed limit
# (as LIMIT clause), and the returned ResidualPlan must contain only the
# operations that were not pushed.
#
# Validates: Requirements 2.5

from rivet_core.optimizer import (
    Cast,
    CastPushdownResult,
    LimitPushdownResult,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    PushdownPlan,
    ResidualPlan,
)
from rivet_core.sql_parser import Predicate

_column_name = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)

_predicate = _column_name.map(
    lambda c: Predicate(expression=f"{c} > 0", columns=[c], location="where")
)

_cast = st.tuples(_column_name, st.just("string"), st.just("int")).map(
    lambda t: Cast(column=t[0], from_type=t[1], to_type=t[2])
)


@st.composite
def _pushdown_plan(draw: st.DrawFn) -> PushdownPlan:
    """Generate a random PushdownPlan with a mix of pushed and residual ops."""
    pushed_cols = draw(st.one_of(st.none(), st.lists(_column_name, min_size=1, max_size=5, unique=True)))
    pushed_preds = draw(st.lists(_predicate, max_size=3))
    residual_preds = draw(st.lists(_predicate, max_size=3))
    pushed_limit = draw(st.one_of(st.none(), st.integers(min_value=1, max_value=10000)))
    residual_limit = draw(st.one_of(st.none(), st.integers(min_value=1, max_value=10000)))
    pushed_casts = draw(st.lists(_cast, max_size=2))
    residual_casts = draw(st.lists(_cast, max_size=2))

    return PushdownPlan(
        predicates=PredicatePushdownResult(pushed=pushed_preds, residual=residual_preds),
        projections=ProjectionPushdownResult(pushed_columns=pushed_cols, reason=None),
        limit=LimitPushdownResult(pushed_limit=pushed_limit, residual_limit=residual_limit, reason=None),
        casts=CastPushdownResult(pushed=pushed_casts, residual=residual_casts),
    )


@settings(max_examples=100)
@given(table=_three_part_table, pushdown=_pushdown_plan())
def test_pushdown_projections_replace_star(table: str, pushdown: PushdownPlan) -> None:
    """Pushed projections replace SELECT * with column list."""
    ref = _make_deferred_ref(None, None)
    sql, _ = _build_read_sql(table, ref, pushdown)

    if pushdown.projections.pushed_columns is not None:
        cols = ", ".join(pushdown.projections.pushed_columns)
        assert sql.startswith(f"SELECT {cols} FROM {table}")
        assert "SELECT *" not in sql
    else:
        assert sql.startswith(f"SELECT * FROM {table}")


@settings(max_examples=100)
@given(table=_three_part_table, pushdown=_pushdown_plan())
def test_pushdown_predicates_as_where_clause(table: str, pushdown: PushdownPlan) -> None:
    """Pushed predicates appear as WHERE clause in SQL."""
    ref = _make_deferred_ref(None, None)
    sql, _ = _build_read_sql(table, ref, pushdown)

    if pushdown.predicates.pushed:
        expected_where = " AND ".join(p.expression for p in pushdown.predicates.pushed)
        assert f"WHERE {expected_where}" in sql
    else:
        assert "WHERE" not in sql


@settings(max_examples=100)
@given(table=_three_part_table, pushdown=_pushdown_plan())
def test_pushdown_limit_as_limit_clause(table: str, pushdown: PushdownPlan) -> None:
    """Pushed limit appears as LIMIT clause in SQL."""
    ref = _make_deferred_ref(None, None)
    sql, _ = _build_read_sql(table, ref, pushdown)

    if pushdown.limit.pushed_limit is not None:
        assert f"LIMIT {pushdown.limit.pushed_limit}" in sql
    else:
        assert "LIMIT" not in sql


@settings(max_examples=100)
@given(table=_three_part_table, pushdown=_pushdown_plan())
def test_pushdown_residual_contains_only_non_pushed(table: str, pushdown: PushdownPlan) -> None:
    """Residual plan contains exactly the residual operations from the pushdown plan."""
    ref = _make_deferred_ref(None, None)
    _, residual = _build_read_sql(table, ref, pushdown)

    assert residual.predicates == list(pushdown.predicates.residual)
    assert residual.limit == pushdown.limit.residual_limit
    assert residual.casts == list(pushdown.casts.residual)


@settings(max_examples=100)
@given(table=_three_part_table, pushdown=_pushdown_plan(), version=_version, timestamp=_timestamp)
def test_pushdown_with_time_travel_preserves_both(
    table: str, pushdown: PushdownPlan, version: int | None, timestamp: str | None
) -> None:
    """Pushdown and time travel clauses coexist correctly in generated SQL."""
    ref = _make_deferred_ref(version, timestamp)
    sql, _ = _build_read_sql(table, ref, pushdown)

    # Time travel still present
    if version is not None:
        assert f"VERSION AS OF {version}" in sql
    elif timestamp is not None:
        assert f"TIMESTAMP AS OF '{timestamp}'" in sql

    # Pushdown still applied
    if pushdown.limit.pushed_limit is not None:
        assert f"LIMIT {pushdown.limit.pushed_limit}" in sql


# ── Property 4: Write dispatch returns material unchanged ─────────────
# Feature: databricks-unity-adapter, Property 4: Write dispatch returns material unchanged
#
# For any material passed to write_dispatch, the returned value must be the
# same material object (identity).
#
# Validates: Requirements 3.3


_material_name = st.from_regex(r"[a-z][a-z0-9_]{0,20}", fullmatch=True)


def _make_material(name: str) -> MagicMock:
    """Create a mock material with a to_arrow() that returns a small PyArrow table."""
    import pyarrow

    mat = MagicMock()
    mat.name = name
    mat.to_arrow.return_value = pyarrow.table({"col": [1]})
    return mat


@settings(max_examples=100)
@given(
    mat_name=_material_name,
    joint_name=_joint_name,
    cat_name=_catalog_name,
    cat_opt=_catalog_name,
    schema_opt=_catalog_name,
)
@patch("rivet_databricks.engine.DatabricksStatementAPI")
def test_write_dispatch_returns_same_material_object(
    mock_api_cls: MagicMock,
    mat_name: str,
    joint_name: str,
    cat_name: str,
    cat_opt: str,
    schema_opt: str,
) -> None:
    """write_dispatch returns the exact same material object (identity check)."""
    from rivet_databricks.adapters.unity import DatabricksUnityAdapter

    table = f"{cat_opt}.{schema_opt}.{joint_name}"
    engine = _make_engine()
    catalog = _make_catalog(cat_name, cat_opt, schema_opt)
    joint = _make_joint(joint_name, table)
    joint.write_strategy = "replace"
    material = _make_material(mat_name)

    adapter = DatabricksUnityAdapter()
    result = adapter.write_dispatch(engine, catalog, joint, material)

    assert result is material


# ── Property 5: Missing credentials raise RVT-501 ────────────────────
# Feature: databricks-unity-adapter, Property 5: Missing credentials raise RVT-501
#
# For any engine config that is missing workspace_url, token, or warehouse_id,
# calling read_dispatch or write_dispatch must raise an ExecutionError with
# error code RVT-501.
#
# Validates: Requirements 4.2

import pytest

from rivet_core.errors import ExecutionError

# Strategy: generate configs where at least one required field is missing.
# We draw each field as either a valid string or None, then filter to ensure
# at least one is None (i.e. missing).
_valid_url = st.just("https://test.databricks.com")
_valid_token = st.just("dapi_test_token")
_valid_wh = st.just("warehouse_123")
_missing = st.just(None)

def _field_value(valid):
    return st.one_of(valid, _missing)


@st.composite
def _incomplete_config(draw: st.DrawFn) -> dict[str, str | None]:
    """Generate an engine config dict with at least one required field missing."""
    workspace_url = draw(_field_value(_valid_url))
    token = draw(_field_value(_valid_token))
    warehouse_id = draw(_field_value(_valid_wh))
    # At least one must be missing
    from hypothesis import assume

    assume(workspace_url is None or token is None or warehouse_id is None)
    cfg: dict[str, str | None] = {}
    if workspace_url is not None:
        cfg["workspace_url"] = workspace_url
    if token is not None:
        cfg["token"] = token
    if warehouse_id is not None:
        cfg["warehouse_id"] = warehouse_id
    return cfg


def _make_engine_from_config(cfg: dict[str, str | None]) -> MagicMock:
    engine = MagicMock()
    engine.config = cfg
    return engine


@settings(max_examples=100)
@given(cfg=_incomplete_config())
def test_missing_credentials_read_dispatch_raises_rvt501(cfg: dict[str, str | None]) -> None:
    """read_dispatch raises ExecutionError with RVT-501 when credentials are missing."""
    from rivet_databricks.adapters.unity import DatabricksUnityAdapter

    engine = _make_engine_from_config(cfg)
    catalog = _make_catalog("cat", "c", "s")
    joint = _make_joint("j", "c.s.t")

    with pytest.raises(ExecutionError) as exc_info:
        DatabricksUnityAdapter().read_dispatch(engine, catalog, joint)
    assert exc_info.value.error.code == "RVT-501"


@settings(max_examples=100)
@given(cfg=_incomplete_config())
def test_missing_credentials_write_dispatch_raises_rvt501(cfg: dict[str, str | None]) -> None:
    """write_dispatch raises ExecutionError with RVT-501 when credentials are missing."""
    from rivet_databricks.adapters.unity import DatabricksUnityAdapter

    engine = _make_engine_from_config(cfg)
    catalog = _make_catalog("cat", "c", "s")
    joint = _make_joint("j", "c.s.t")
    material = _make_material("m")

    with pytest.raises(ExecutionError) as exc_info:
        DatabricksUnityAdapter().write_dispatch(engine, catalog, joint, material)
    assert exc_info.value.error.code == "RVT-501"


# ── Property 6: Table name resolution and three-part validation ───────
# Feature: databricks-unity-adapter, Property 6: Table name resolution and three-part validation
#
# For any joint and catalog combination, if joint.table is set it must be used
# directly as the table reference; if not, the table name must be resolved via
# UnityCatalogPlugin.default_table_reference(). In either case, if the resolved
# name does not contain exactly two dots (three parts), an ExecutionError with
# code RVT-503 must be raised.
#
# Validates: Requirements 5.1, 5.2, 5.3

from rivet_databricks.adapters.unity import _resolve_table_name


@settings(max_examples=100)
@given(
    cat=_name_part,
    schema=_name_part,
    tbl=_name_part,
)
def test_joint_table_used_directly_when_set(cat: str, schema: str, tbl: str) -> None:
    """When joint.table is set, it is used directly as the table reference."""
    three_part = f"{cat}.{schema}.{tbl}"
    joint = _make_joint("ignored", three_part)
    catalog = MagicMock()
    assert _resolve_table_name(joint, catalog) == three_part


@settings(max_examples=100)
@given(
    joint_name=_name_part,
    cat_opt=_name_part,
    schema_opt=_name_part,
)
def test_resolves_via_default_table_reference_when_no_table(
    joint_name: str, cat_opt: str, schema_opt: str
) -> None:
    """When joint.table is not set, resolves via UnityCatalogPlugin.default_table_reference()."""
    joint = MagicMock()
    joint.table = None
    joint.name = joint_name
    catalog = MagicMock()
    catalog.options = {"catalog_name": cat_opt, "schema": schema_opt}

    result = _resolve_table_name(joint, catalog)
    assert result == f"{cat_opt}.{schema_opt}.{joint_name}"


# Strategy: generate table names that are NOT three-part (0, 1, 2, or 4+ dots)
_invalid_table_name = st.one_of(
    _name_part,  # no dots (1 part)
    st.tuples(_name_part, _name_part).map(lambda t: f"{t[0]}.{t[1]}"),  # 2 parts
    st.tuples(_name_part, _name_part, _name_part, _name_part).map(
        lambda t: f"{t[0]}.{t[1]}.{t[2]}.{t[3]}"
    ),  # 4 parts
)


@settings(max_examples=100)
@given(invalid_name=_invalid_table_name)
def test_invalid_table_name_raises_rvt503(invalid_name: str) -> None:
    """Non-three-part table names raise ExecutionError with RVT-503."""
    joint = _make_joint("j", invalid_name)
    catalog = MagicMock()

    with pytest.raises(ExecutionError) as exc_info:
        _resolve_table_name(joint, catalog)
    assert exc_info.value.error.code == "RVT-503"


@settings(max_examples=100)
@given(
    cat=_name_part,
    schema=_name_part,
    tbl=_name_part,
)
def test_valid_three_part_name_does_not_raise(cat: str, schema: str, tbl: str) -> None:
    """Valid three-part table names do not raise."""
    three_part = f"{cat}.{schema}.{tbl}"
    joint = _make_joint("j", three_part)
    catalog = MagicMock()
    result = _resolve_table_name(joint, catalog)
    assert result == three_part


# ── Property 7: Error context completeness ────────────────────────────
# Feature: databricks-unity-adapter, Property 7: Error context completeness
#
# For any error raised by the DatabricksUnityAdapter, the error's context
# dictionary must contain plugin_name="rivet_databricks", plugin_type="adapter",
# and adapter="DatabricksUnityAdapter".
#
# Validates: Requirements 6.4

_REQUIRED_CONTEXT = {
    "plugin_name": "rivet_databricks",
    "plugin_type": "adapter",
    "adapter": "DatabricksUnityAdapter",
}


def _assert_error_context(exc_info: pytest.ExceptionInfo[ExecutionError]) -> None:
    ctx = exc_info.value.error.context
    for key, value in _REQUIRED_CONTEXT.items():
        assert key in ctx, f"Missing '{key}' in error context: {ctx}"
        assert ctx[key] == value, f"Expected {key}={value!r}, got {ctx[key]!r}"


@settings(max_examples=100)
@given(cfg=_incomplete_config())
def test_error_context_missing_credentials_read(cfg: dict[str, str | None]) -> None:
    """RVT-501 from missing credentials on read_dispatch has required context fields."""
    from rivet_databricks.adapters.unity import DatabricksUnityAdapter

    engine = _make_engine_from_config(cfg)
    catalog = _make_catalog("cat", "c", "s")
    joint = _make_joint("j", "c.s.t")

    with pytest.raises(ExecutionError) as exc_info:
        DatabricksUnityAdapter().read_dispatch(engine, catalog, joint)
    _assert_error_context(exc_info)


@settings(max_examples=100)
@given(cfg=_incomplete_config())
def test_error_context_missing_credentials_write(cfg: dict[str, str | None]) -> None:
    """RVT-501 from missing credentials on write_dispatch has required context fields."""
    from rivet_databricks.adapters.unity import DatabricksUnityAdapter

    engine = _make_engine_from_config(cfg)
    catalog = _make_catalog("cat", "c", "s")
    joint = _make_joint("j", "c.s.t")
    material = _make_material("m")

    with pytest.raises(ExecutionError) as exc_info:
        DatabricksUnityAdapter().write_dispatch(engine, catalog, joint, material)
    _assert_error_context(exc_info)


@settings(max_examples=100)
@given(invalid_name=_invalid_table_name)
def test_error_context_invalid_table_name(invalid_name: str) -> None:
    """RVT-503 from invalid table name has required context fields."""
    joint = _make_joint("j", invalid_name)
    catalog = MagicMock()

    with pytest.raises(ExecutionError) as exc_info:
        _resolve_table_name(joint, catalog)
    _assert_error_context(exc_info)


@settings(max_examples=100)
@given(joint_name=_joint_name, cat_opt=_catalog_name, schema_opt=_catalog_name)
@patch("rivet_databricks.engine.DatabricksStatementAPI")
def test_error_context_connection_error_write(
    mock_api_cls: MagicMock,
    joint_name: str,
    cat_opt: str,
    schema_opt: str,
) -> None:
    """RVT-501 from ConnectionError on write_dispatch has required context fields."""
    import requests

    from rivet_databricks.adapters.unity import DatabricksUnityAdapter

    mock_api_cls.return_value.execute.side_effect = requests.ConnectionError("fail")
    table = f"{cat_opt}.{schema_opt}.{joint_name}"
    engine = _make_engine()
    catalog = _make_catalog("cat", cat_opt, schema_opt)
    joint = _make_joint(joint_name, table)
    joint.write_strategy = "replace"
    material = _make_material("m")

    with pytest.raises(ExecutionError) as exc_info:
        DatabricksUnityAdapter().write_dispatch(engine, catalog, joint, material)
    _assert_error_context(exc_info)
