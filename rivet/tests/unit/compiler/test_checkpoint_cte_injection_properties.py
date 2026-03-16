"""Property-based tests for checkpoint CTE injection.

Properties:
  1. Injected checkpoint CTEs are always prepended before existing CTEs
  2. Existing SQL content is preserved after injection

**Validates: Requirements 1.1, 1.2, 1.3, 5.1**
"""

from __future__ import annotations

from hypothesis import assume, given, settings
from hypothesis import strategies as st

from rivet_core.compiler import (
    CompiledJoint,
    _inject_checkpoint_ctes,
    _prepend_ctes,
)
from rivet_core.models import Catalog
from rivet_core.optimizer import CheckpointSourceInfo, FusedGroup, FusionResult

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Valid SQL identifier: starts with letter, followed by letters/digits/underscores
_sql_identifier = st.from_regex(r"[a-z][a-z0-9_]{2,15}", fullmatch=True)

# 3-part fully-qualified table name
_fq_table_name = st.builds(
    lambda cat, sch, tbl: f"{cat}.{sch}.{tbl}",
    cat=st.from_regex(r"[a-z][a-z0-9_]{2,10}", fullmatch=True),
    sch=st.from_regex(r"[a-z][a-z0-9_]{2,10}", fullmatch=True),
    tbl=st.from_regex(r"[a-z][a-z0-9_]{2,10}", fullmatch=True),
)


@st.composite
def fused_sql_strategy(draw: st.DrawFn) -> tuple[str, list[str]]:
    """Generate random fused SQL with optional WITH clause.

    Returns (sql, cte_names) where cte_names is the list of existing CTE names.
    """
    # Generate 0-3 existing CTE names
    n_ctes = draw(st.integers(min_value=0, max_value=3))
    cte_names = [
        draw(st.from_regex(r"[a-z][a-z0-9_]{2,10}", fullmatch=True)) for _ in range(n_ctes)
    ]
    # Make unique
    cte_names = list(dict.fromkeys(cte_names))

    final_select = f"SELECT * FROM {cte_names[-1] if cte_names else 'some_table'}"

    if not cte_names:
        return final_select, []

    cte_parts = [f"{name} AS (\n    SELECT 1 AS col_{name}\n)" for name in cte_names]
    sql = "WITH " + ",\n".join(cte_parts) + "\n" + final_select
    return sql, cte_names


@st.composite
def checkpoint_cte_parts_strategy(draw: st.DrawFn) -> tuple[list[str], list[str]]:
    """Generate random checkpoint CTE parts.

    Returns (cte_parts, checkpoint_names) where cte_parts are the CTE strings
    and checkpoint_names are the checkpoint names.
    """
    n_checkpoints = draw(st.integers(min_value=1, max_value=3))
    checkpoint_names: list[str] = []
    cte_parts: list[str] = []

    for _ in range(n_checkpoints):
        name = draw(_sql_identifier.filter(lambda x: x not in checkpoint_names))
        checkpoint_names.append(name)
        fq_name = draw(_fq_table_name)
        cte_parts.append(f"{name} AS (\n    SELECT * FROM {fq_name}\n)")

    return cte_parts, checkpoint_names


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _checkpoint_cj(
    name: str = "bu_prep",
    catalog: str | None = "unity",
    catalog_type: str | None = "unity",
    table: str | None = "rvt_bu_prep",
) -> CompiledJoint:
    """Minimal CompiledJoint for checkpoint CTE injection tests."""
    return CompiledJoint(
        name=name,
        type="checkpoint",
        catalog=catalog,
        catalog_type=catalog_type,
        engine="databricks",
        engine_resolution="project_default",
        adapter="databricks:unity",
        sql="SELECT * FROM upstream",
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=["upstream_joint"],
        eager=False,
        table=table,
        write_strategy="replace",
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


def _catalog(
    name: str = "unity",
    type: str = "unity",
    options: dict[str, str] | None = None,
) -> Catalog:
    return Catalog(name=name, type=type, options=options or {})


def _fused_group(
    group_id: str = "group-1",
    joints: list[str] | None = None,
    fused_sql: str | None = None,
    resolved_sql: str | None = None,
    fusion_result: FusionResult | None = None,
    checkpoint_sources: dict[str, CheckpointSourceInfo] | None = None,
) -> FusedGroup:
    return FusedGroup(
        id=group_id,
        joints=joints or ["sql_joint_1", "sink_1"],
        engine="databricks",
        engine_type="databricks",
        adapters={"sql_joint_1": None},
        fused_sql=fused_sql,
        fusion_result=fusion_result,
        resolved_sql=resolved_sql,
        checkpoint_sources=checkpoint_sources or {},
    )


# ---------------------------------------------------------------------------
# Property 1: Injected checkpoint CTEs are always prepended before existing CTEs
# **Validates: Requirements 1.1, 1.2, 1.3**
# ---------------------------------------------------------------------------


@given(
    fused_sql_data=fused_sql_strategy(),
    checkpoint_data=checkpoint_cte_parts_strategy(),
)
@settings(max_examples=50)
def test_property_checkpoint_ctes_prepended_before_existing(
    fused_sql_data: tuple[str, list[str]],
    checkpoint_data: tuple[list[str], list[str]],
) -> None:
    """Injected checkpoint CTEs are always prepended before existing CTEs.

    **Validates: Requirements 1.1, 1.2, 1.3**
    """
    fused_sql, existing_cte_names = fused_sql_data
    cte_parts, checkpoint_names = checkpoint_data

    # Ensure checkpoint names don't collide with existing CTE names
    assume(not set(checkpoint_names) & set(existing_cte_names))

    result = _prepend_ctes(fused_sql, cte_parts)

    # Result must start with "WITH "
    assert result.upper().startswith("WITH "), f"Result should start with WITH: {result}"

    # All checkpoint CTE names must appear in the result
    for cp_name in checkpoint_names:
        assert f"{cp_name} AS" in result, f"Checkpoint {cp_name} not found in result"

    # Checkpoint CTEs must appear BEFORE any original CTEs
    for cp_name in checkpoint_names:
        cp_pos = result.index(f"{cp_name} AS")
        for existing_name in existing_cte_names:
            existing_pos = result.index(f"{existing_name} AS")
            assert cp_pos < existing_pos, (
                f"Checkpoint {cp_name} (pos {cp_pos}) should appear before "
                f"existing CTE {existing_name} (pos {existing_pos})"
            )


@given(
    checkpoint_names=st.lists(_sql_identifier, min_size=1, max_size=3, unique=True),
    catalog_name=st.from_regex(r"[a-z][a-z0-9_]{2,10}", fullmatch=True),
    schema_name=st.from_regex(r"[a-z][a-z0-9_]{2,10}", fullmatch=True),
    fused_sql_data=fused_sql_strategy(),
)
@settings(max_examples=50)
def test_property_inject_checkpoint_ctes_prepends_all(
    checkpoint_names: list[str],
    catalog_name: str,
    schema_name: str,
    fused_sql_data: tuple[str, list[str]],
) -> None:
    """_inject_checkpoint_ctes prepends all checkpoint CTEs before existing CTEs.

    **Validates: Requirements 1.1, 1.2, 1.3**
    """
    fused_sql, existing_cte_names = fused_sql_data

    # Ensure checkpoint names don't collide with existing CTE names
    assume(not set(checkpoint_names) & set(existing_cte_names))

    # Build cj_map and catalog_map
    cj_map: dict[str, CompiledJoint] = {}
    checkpoint_sources: dict[str, CheckpointSourceInfo] = {}

    for cp_name in checkpoint_names:
        table_name = f"rvt_{cp_name}"
        cj_map[cp_name] = _checkpoint_cj(name=cp_name, table=table_name)
        checkpoint_sources[cp_name] = CheckpointSourceInfo(
            checkpoint_joint=cp_name,
            catalog="unity",
            catalog_type="unity",
            table=table_name,
            adapter="databricks:unity",
        )

    catalog_map = {"unity": _catalog(options={"catalog_name": catalog_name, "schema": schema_name})}

    group = _fused_group(
        fused_sql=fused_sql,
        checkpoint_sources=checkpoint_sources,
    )

    result = _inject_checkpoint_ctes([group], cj_map, catalog_map)

    assert len(result) == 1
    result_sql = result[0].fused_sql
    assert result_sql is not None

    # Result must start with "WITH "
    assert result_sql.upper().startswith("WITH "), f"Result should start with WITH: {result_sql}"

    # All checkpoint CTE names must appear in the result
    for cp_name in checkpoint_names:
        assert f"{cp_name} AS" in result_sql, f"Checkpoint {cp_name} not found in result"

    # Checkpoint CTEs must appear BEFORE any original CTEs
    for cp_name in checkpoint_names:
        cp_pos = result_sql.index(f"{cp_name} AS")
        for existing_name in existing_cte_names:
            existing_pos = result_sql.index(f"{existing_name} AS")
            assert cp_pos < existing_pos, (
                f"Checkpoint {cp_name} (pos {cp_pos}) should appear before "
                f"existing CTE {existing_name} (pos {existing_pos})"
            )


# ---------------------------------------------------------------------------
# Property 2: Existing SQL content is preserved after injection
# **Validates: Requirements 1.3, 5.1**
# ---------------------------------------------------------------------------


@given(
    fused_sql_data=fused_sql_strategy(),
    checkpoint_data=checkpoint_cte_parts_strategy(),
)
@settings(max_examples=50)
def test_property_existing_sql_content_preserved(
    fused_sql_data: tuple[str, list[str]],
    checkpoint_data: tuple[list[str], list[str]],
) -> None:
    """Existing SQL content is preserved after injection.

    **Validates: Requirements 1.3, 5.1**
    """
    fused_sql, existing_cte_names = fused_sql_data
    cte_parts, checkpoint_names = checkpoint_data

    # Ensure checkpoint names don't collide with existing CTE names
    assume(not set(checkpoint_names) & set(existing_cte_names))

    result = _prepend_ctes(fused_sql, cte_parts)

    # All original CTE bodies must be present and unmodified
    for cte_name in existing_cte_names:
        cte_body = f"SELECT 1 AS col_{cte_name}"
        assert cte_body in result, f"Original CTE body for {cte_name} not found in result"

    # The final SELECT must be present and unmodified
    if existing_cte_names:
        final_select = f"SELECT * FROM {existing_cte_names[-1]}"
    else:
        final_select = "SELECT * FROM some_table"
    assert final_select in result, f"Final SELECT not found in result: {final_select}"


@given(
    checkpoint_names=st.lists(_sql_identifier, min_size=1, max_size=3, unique=True),
    catalog_name=st.from_regex(r"[a-z][a-z0-9_]{2,10}", fullmatch=True),
    schema_name=st.from_regex(r"[a-z][a-z0-9_]{2,10}", fullmatch=True),
    fused_sql_data=fused_sql_strategy(),
)
@settings(max_examples=50)
def test_property_inject_preserves_existing_content(
    checkpoint_names: list[str],
    catalog_name: str,
    schema_name: str,
    fused_sql_data: tuple[str, list[str]],
) -> None:
    """_inject_checkpoint_ctes preserves all existing SQL content.

    **Validates: Requirements 1.3, 5.1**
    """
    fused_sql, existing_cte_names = fused_sql_data

    # Ensure checkpoint names don't collide with existing CTE names
    assume(not set(checkpoint_names) & set(existing_cte_names))

    # Build cj_map and catalog_map
    cj_map: dict[str, CompiledJoint] = {}
    checkpoint_sources: dict[str, CheckpointSourceInfo] = {}

    for cp_name in checkpoint_names:
        table_name = f"rvt_{cp_name}"
        cj_map[cp_name] = _checkpoint_cj(name=cp_name, table=table_name)
        checkpoint_sources[cp_name] = CheckpointSourceInfo(
            checkpoint_joint=cp_name,
            catalog="unity",
            catalog_type="unity",
            table=table_name,
            adapter="databricks:unity",
        )

    catalog_map = {"unity": _catalog(options={"catalog_name": catalog_name, "schema": schema_name})}

    group = _fused_group(
        fused_sql=fused_sql,
        checkpoint_sources=checkpoint_sources,
    )

    result = _inject_checkpoint_ctes([group], cj_map, catalog_map)

    assert len(result) == 1
    result_sql = result[0].fused_sql
    assert result_sql is not None

    # All original CTE bodies must be present and unmodified
    for cte_name in existing_cte_names:
        cte_body = f"SELECT 1 AS col_{cte_name}"
        assert cte_body in result_sql, f"Original CTE body for {cte_name} not found in result"

    # The final SELECT must be present and unmodified
    if existing_cte_names:
        final_select = f"SELECT * FROM {existing_cte_names[-1]}"
    else:
        final_select = "SELECT * FROM some_table"
    assert final_select in result_sql, f"Final SELECT not found in result: {final_select}"


@given(
    checkpoint_names=st.lists(_sql_identifier, min_size=1, max_size=3, unique=True),
    catalog_name=st.from_regex(r"[a-z][a-z0-9_]{2,10}", fullmatch=True),
    schema_name=st.from_regex(r"[a-z][a-z0-9_]{2,10}", fullmatch=True),
    fused_sql_data=fused_sql_strategy(),
)
@settings(max_examples=50)
def test_property_fusion_result_fields_preserved(
    checkpoint_names: list[str],
    catalog_name: str,
    schema_name: str,
    fused_sql_data: tuple[str, list[str]],
) -> None:
    """_inject_checkpoint_ctes preserves fusion_result fields correctly.

    **Validates: Requirements 1.3, 5.1**
    """
    fused_sql, existing_cte_names = fused_sql_data

    # Ensure checkpoint names don't collide with existing CTE names
    assume(not set(checkpoint_names) & set(existing_cte_names))

    # Build cj_map and catalog_map
    cj_map: dict[str, CompiledJoint] = {}
    checkpoint_sources: dict[str, CheckpointSourceInfo] = {}

    for cp_name in checkpoint_names:
        table_name = f"rvt_{cp_name}"
        cj_map[cp_name] = _checkpoint_cj(name=cp_name, table=table_name)
        checkpoint_sources[cp_name] = CheckpointSourceInfo(
            checkpoint_joint=cp_name,
            catalog="unity",
            catalog_type="unity",
            table=table_name,
            adapter="databricks:unity",
        )

    catalog_map = {"unity": _catalog(options={"catalog_name": catalog_name, "schema": schema_name})}

    # Build statements from existing CTEs
    if existing_cte_names:
        statements = [f"{name} AS (\n    SELECT 1 AS col_{name}\n)" for name in existing_cte_names]
        final_select = f"SELECT * FROM {existing_cte_names[-1]}"
    else:
        statements = []
        final_select = "SELECT * FROM some_table"

    fusion_result = FusionResult(
        fused_sql=fused_sql,
        statements=statements,
        final_select=final_select,
    )

    group = _fused_group(
        fused_sql=fused_sql,
        fusion_result=fusion_result,
        checkpoint_sources=checkpoint_sources,
    )

    result = _inject_checkpoint_ctes([group], cj_map, catalog_map)

    assert len(result) == 1
    fr = result[0].fusion_result
    assert fr is not None

    # final_select must be unchanged
    assert fr.final_select == final_select, (
        f"final_select changed: {fr.final_select} != {final_select}"
    )

    # Original statements must be preserved (at the end)
    n_checkpoints = len(checkpoint_names)
    original_statements = fr.statements[n_checkpoints:]
    assert original_statements == statements, (
        f"Original statements not preserved: {original_statements} != {statements}"
    )

    # Checkpoint statements must be prepended
    checkpoint_statements = fr.statements[:n_checkpoints]
    for i, cp_name in enumerate(checkpoint_names):
        assert f"{cp_name} AS" in checkpoint_statements[i], (
            f"Checkpoint {cp_name} not in statement {i}: {checkpoint_statements[i]}"
        )
