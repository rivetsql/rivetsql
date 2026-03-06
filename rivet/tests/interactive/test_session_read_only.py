"""Property test for InteractiveSession read-only mode.

# Feature: cli-repl, Property 14: Read-only mode rejects all execution
# Validates: Requirements 1.6

For any InteractiveSession created with read_only=True:
- execute_query(), execute_joint(), execute_pipeline() MUST raise ReadOnlyError
- compile(), get_completions(), search_catalog(), format_sql(),
  profile_result(), diff_results() MUST succeed (not raise ReadOnlyError)
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.compiler import (
    CompiledAssembly,
    CompiledCatalog,
    CompiledEngine,
    CompiledJoint,
)
from rivet_core.interactive.session import InteractiveSession, ReadOnlyError
from rivet_core.plugins import PluginRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_compiled_joint(name: str) -> CompiledJoint:
    return CompiledJoint(
        name=name,
        type="sql",
        catalog=None,
        catalog_type=None,
        engine="eng",
        engine_resolution="project_default",
        adapter=None,
        sql="SELECT 1 AS x",
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=[],
        eager=False,
        table=None,
        write_strategy=None,
        function=None,
        source_file=None,
        logical_plan=None,
        output_schema=None,
        column_lineage=[],
        optimizations=[],
        checks=[],
        fused_group_id=None,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _make_compiled_assembly(joint_names: list[str] | None = None) -> CompiledAssembly:
    joints = [_make_compiled_joint(n) for n in (joint_names or ["j1"])]
    return CompiledAssembly(
        success=True,
        profile_name="default",
        catalogs=[CompiledCatalog(name="cat", type="stub")],
        engines=[CompiledEngine(name="eng", engine_type="stub", native_catalog_types=[])],
        adapters=[],
        joints=joints,
        fused_groups=[],
        materializations=[],
        execution_order=[j.name for j in joints],
        errors=[],
        warnings=[],
    )


def _make_read_only_session() -> InteractiveSession:
    """Return a started read-only session with a minimal compiled assembly."""
    session = InteractiveSession(project_path=None, read_only=True)  # type: ignore[arg-type]
    # Bypass start() by directly injecting state
    session._assembly = _make_compiled_assembly()
    session._raw_assembly = Assembly(joints=[])
    session._registry = PluginRegistry()
    return session


# ---------------------------------------------------------------------------
# Property 14a: execution operations raise ReadOnlyError
# ---------------------------------------------------------------------------

@given(sql=st.text(min_size=1, max_size=200))
@settings(max_examples=50)
def test_execute_query_raises_in_read_only(sql: str) -> None:
    """execute_query() must raise ReadOnlyError for any SQL in read-only mode."""
    session = _make_read_only_session()
    with pytest.raises(ReadOnlyError):
        session.execute_query(sql)


@given(joint_name=st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_")))
@settings(max_examples=50)
def test_execute_joint_raises_in_read_only(joint_name: str) -> None:
    """execute_joint() must raise ReadOnlyError for any joint name in read-only mode."""
    session = _make_read_only_session()
    with pytest.raises(ReadOnlyError):
        session.execute_joint(joint_name)


def test_execute_pipeline_raises_in_read_only() -> None:
    """execute_pipeline() must raise ReadOnlyError in read-only mode."""
    session = _make_read_only_session()
    with pytest.raises(ReadOnlyError):
        session.execute_pipeline()


# ---------------------------------------------------------------------------
# Property 14b: inspection operations succeed in read-only mode
# ---------------------------------------------------------------------------

def test_get_completions_succeeds_in_read_only() -> None:
    """get_completions() must not raise ReadOnlyError in read-only mode."""
    session = _make_read_only_session()
    result = session.get_completions("SELECT ", 7)
    assert isinstance(result, list)


def test_search_catalog_succeeds_in_read_only() -> None:
    """search_catalog() must not raise ReadOnlyError in read-only mode."""
    session = _make_read_only_session()
    result = session.search_catalog("orders")
    assert isinstance(result, list)


def test_format_sql_succeeds_in_read_only() -> None:
    """format_sql() must not raise ReadOnlyError in read-only mode."""
    session = _make_read_only_session()
    result = session.format_sql("select 1")
    assert isinstance(result, str)


def test_profile_result_succeeds_in_read_only() -> None:
    """profile_result() must not raise ReadOnlyError in read-only mode."""
    session = _make_read_only_session()
    table = pa.table({"x": [1, 2, 3]})
    result = session.profile_result(table)
    assert result.row_count == 3


def test_diff_results_succeeds_in_read_only() -> None:
    """diff_results() must not raise ReadOnlyError in read-only mode."""
    session = _make_read_only_session()
    t1 = pa.table({"id": [1, 2], "v": ["a", "b"]})
    t2 = pa.table({"id": [1, 3], "v": ["a", "c"]})
    result = session.diff_results(t1, t2, key_columns=["id"])
    assert result is not None


def test_compile_succeeds_in_read_only() -> None:
    """compile() must not raise ReadOnlyError in read-only mode."""
    session = _make_read_only_session()
    # compile() requires _raw_assembly and _registry — both set in helper
    result = session.compile()
    assert result is not None
