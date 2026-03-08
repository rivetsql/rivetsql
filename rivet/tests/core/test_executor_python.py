"""Tests for PythonJoint execution (task 14.3)."""
from __future__ import annotations

import asyncio

import pyarrow
import pytest

from rivet_core.compiler import (
    CompiledAssembly,
    CompiledEngine,
    CompiledJoint,
    Materialization,
)
from rivet_core.errors import ExecutionError
from rivet_core.executor import Executor, _normalize_python_result
from rivet_core.models import Material
from rivet_core.optimizer import FusedGroup

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FIXTURE_MOD = "tests.core._python_joint_fixtures"


def _cj(
    name: str,
    joint_type: str = "python",
    upstream: list[str] | None = None,
    fused_group_id: str | None = None,
    **kwargs: object,
) -> CompiledJoint:
    defaults = dict(
        catalog=None, catalog_type=None, engine="arrow",
        engine_resolution="project_default", adapter=None,
        sql=None, sql_translated=None, sql_resolved=None,
        sql_dialect=None, engine_dialect=None,
        upstream=upstream or [], eager=False, table=None,
        write_strategy=None, function=None, source_file=None,
        logical_plan=None, output_schema=None, column_lineage=[],
        optimizations=[], checks=[], fused_group_id=fused_group_id,
        tags=[], description=None, fusion_strategy_override=None,
        materialization_strategy_override=None,
    )
    defaults.update(kwargs)
    return CompiledJoint(name=name, type=joint_type, **defaults)  # type: ignore[arg-type]


def _grp(gid: str, joints: list[str]) -> FusedGroup:
    return FusedGroup(
        id=gid, joints=joints, engine="arrow", engine_type="arrow",
        adapters={}, fused_sql=None,
        entry_joints=[joints[0]], exit_joints=[joints[-1]],
    )


def _asm(
    joints: list[CompiledJoint],
    groups: list[FusedGroup],
    materializations: list[Materialization] | None = None,
) -> CompiledAssembly:
    return CompiledAssembly(
        success=True, profile_name="test", catalogs=[],
        engines=[CompiledEngine(name="arrow", engine_type="arrow", native_catalog_types=["arrow"])],
        adapters=[], joints=joints, fused_groups=groups,
        materializations=materializations or [],
        execution_order=[g.id for g in groups],
        errors=[], warnings=[],
    )


def _mat(from_j: str, to_j: str) -> Materialization:
    return Materialization(from_joint=from_j, to_joint=to_j,
                           trigger="python_boundary", detail="", strategy="arrow")


# ---------------------------------------------------------------------------
# Tests: _normalize_python_result (Req 26.3, 26.10)
# ---------------------------------------------------------------------------


class TestNormalizePythonResult:
    def test_arrow_table_passthrough(self) -> None:
        table = pyarrow.table({"x": [1]})
        result = _normalize_python_result("t", "f", table)
        assert isinstance(result, Material)
        assert result.state == "materialized"
        assert result.materialized_ref is not None
        assert result.materialized_ref.to_arrow().equals(table)

    def test_none_raises_rvt752(self) -> None:
        with pytest.raises(ExecutionError) as exc_info:
            _normalize_python_result("t", "f", None)
        assert exc_info.value.error.code == "RVT-752"
        assert "returned None" in exc_info.value.error.message

    def test_unsupported_type_raises_rvt752(self) -> None:
        with pytest.raises(ExecutionError) as exc_info:
            _normalize_python_result("t", "f", "a string")
        assert exc_info.value.error.code == "RVT-752"
        assert "unsupported type" in exc_info.value.error.message


# ---------------------------------------------------------------------------
# Tests: single-input shorthand (Req 26.4)
# ---------------------------------------------------------------------------


class TestPythonJointSingleInput:
    def test_single_upstream_receives_material(self) -> None:
        src = _cj("src", joint_type="source", fused_group_id="g0")
        pj = _cj("pj", upstream=["src"],
                  function=f"{_FIXTURE_MOD}.transform_arrow", fused_group_id="g1")
        asm = _asm([src, pj], [_grp("g0", ["src"]), _grp("g1", ["pj"])], [_mat("src", "pj")])

        result = asyncio.run(Executor().run(asm))
        assert result.success
        pj_r = next(r for r in result.joint_results if r.name == "pj")
        assert pj_r.success


# ---------------------------------------------------------------------------
# Tests: multi-input (Req 26.2)
# ---------------------------------------------------------------------------


class TestPythonJointMultiInput:
    def test_multi_upstream_dict_input(self) -> None:
        s1 = _cj("s1", joint_type="source", fused_group_id="g0")
        s2 = _cj("s2", joint_type="source", fused_group_id="g1")
        pj = _cj("pj", upstream=["s1", "s2"],
                  function=f"{_FIXTURE_MOD}.transform_multi", fused_group_id="g2")
        asm = _asm([s1, s2, pj], [_grp("g0", ["s1"]), _grp("g1", ["s2"]), _grp("g2", ["pj"])])

        result = asyncio.run(Executor().run(asm))
        assert result.success


# ---------------------------------------------------------------------------
# Tests: error handling (Req 26.9, 26.10)
# ---------------------------------------------------------------------------


class TestPythonJointErrors:
    def test_exception_produces_failure(self) -> None:
        src = _cj("src", joint_type="source", fused_group_id="g0")
        pj = _cj("pj", upstream=["src"],
                  function=f"{_FIXTURE_MOD}.transform_raises", fused_group_id="g1")
        asm = _asm([src, pj], [_grp("g0", ["src"]), _grp("g1", ["pj"])], [_mat("src", "pj")])

        result = asyncio.run(Executor().run(asm, fail_fast=True))
        assert not result.success
        pj_r = next(r for r in result.joint_results if r.name == "pj")
        assert not pj_r.success
        assert pj_r.error is not None

    def test_none_return_produces_failure(self) -> None:
        src = _cj("src", joint_type="source", fused_group_id="g0")
        pj = _cj("pj", upstream=["src"],
                  function=f"{_FIXTURE_MOD}.transform_returns_none", fused_group_id="g1")
        asm = _asm([src, pj], [_grp("g0", ["src"]), _grp("g1", ["pj"])], [_mat("src", "pj")])

        result = asyncio.run(Executor().run(asm, fail_fast=True))
        assert not result.success

    def test_unsupported_return_type_produces_failure(self) -> None:
        src = _cj("src", joint_type="source", fused_group_id="g0")
        pj = _cj("pj", upstream=["src"],
                  function=f"{_FIXTURE_MOD}.transform_returns_string", fused_group_id="g1")
        asm = _asm([src, pj], [_grp("g0", ["src"]), _grp("g1", ["pj"])], [_mat("src", "pj")])

        result = asyncio.run(Executor().run(asm, fail_fast=True))
        assert not result.success


# ---------------------------------------------------------------------------
# Tests: async support (Req 26.8)
# ---------------------------------------------------------------------------


class TestPythonJointAsync:
    def test_async_function_awaited(self) -> None:
        src = _cj("src", joint_type="source", fused_group_id="g0")
        pj = _cj("pj", upstream=["src"],
                  function=f"{_FIXTURE_MOD}.transform_async", fused_group_id="g1")
        asm = _asm([src, pj], [_grp("g0", ["src"]), _grp("g1", ["pj"])], [_mat("src", "pj")])

        result = asyncio.run(Executor().run(asm))
        assert result.success


# ---------------------------------------------------------------------------
# Tests: RivetContext (Req 26.2)
# ---------------------------------------------------------------------------


class TestPythonJointContext:
    def test_context_passed_when_annotated(self) -> None:
        src = _cj("src", joint_type="source", fused_group_id="g0")
        pj = _cj("pj", upstream=["src"],
                  function=f"{_FIXTURE_MOD}.transform_with_context", fused_group_id="g1")
        asm = _asm([src, pj], [_grp("g0", ["src"]), _grp("g1", ["pj"])], [_mat("src", "pj")])

        result = asyncio.run(Executor().run(asm))
        assert result.success
