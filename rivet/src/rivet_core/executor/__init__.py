"""Executor package — re-exports all public and test-used symbols.

This ``__init__.py`` uses the ``X as X`` pattern required by mypy strict
mode (``no_implicit_reexport``) so that every symbol listed here is
available via ``from rivet_core.executor import X``.

This is Level 4 of the executor package dependency hierarchy.
"""

from __future__ import annotations

# helpers/arrow_helpers.py
from rivet_core.executor.helpers.arrow_helpers import (
    SAMPLE_THRESHOLD as SAMPLE_THRESHOLD,
)
from rivet_core.executor.helpers.arrow_helpers import (
    _apply_residuals as _apply_residuals,
)
from rivet_core.executor.helpers.arrow_helpers import (
    _apply_source_expressions as _apply_source_expressions,
)
from rivet_core.executor.helpers.arrow_helpers import (
    _apply_source_inline_residuals as _apply_source_inline_residuals,
)
from rivet_core.executor.helpers.arrow_helpers import (
    _compute_materialization_stats as _compute_materialization_stats,
)
from rivet_core.executor.helpers.arrow_helpers import (
    _normalize_arrow_type as _normalize_arrow_type,
)
from rivet_core.executor.helpers.arrow_helpers import (
    _schemas_are_compatible as _schemas_are_compatible,
)

# --- Private symbols re-exported for test code ---
# helpers/checks.py
from rivet_core.executor.helpers.checks import (
    _SQL_TRANSLATABLE_CHECKS as _SQL_TRANSLATABLE_CHECKS,
)
from rivet_core.executor.helpers.checks import _execute_check as _execute_check
from rivet_core.executor.helpers.checks import (
    _generate_check_sql as _generate_check_sql,
)
from rivet_core.executor.helpers.checks import (
    _interpret_check_sql_result as _interpret_check_sql_result,
)
from rivet_core.executor.helpers.checks import (
    _is_sql_translatable as _is_sql_translatable,
)

# helpers/pushdown.py
from rivet_core.executor.helpers.pushdown import (
    _merge_cross_group_limits as _merge_cross_group_limits,
)
from rivet_core.executor.helpers.pushdown import (
    _merge_cross_group_predicates as _merge_cross_group_predicates,
)
from rivet_core.executor.helpers.pushdown import (
    _merge_cross_group_projections as _merge_cross_group_projections,
)
from rivet_core.executor.helpers.pushdown import _merge_residuals as _merge_residuals
from rivet_core.executor.helpers.pushdown import (
    _merge_source_limit_into_pushdown as _merge_source_limit_into_pushdown,
)
from rivet_core.executor.helpers.pushdown import (
    _merge_source_predicates_into_pushdown as _merge_source_predicates_into_pushdown,
)
from rivet_core.executor.helpers.pushdown import (
    _merge_source_projections_into_pushdown as _merge_source_projections_into_pushdown,
)

# helpers/utils.py
from rivet_core.executor.helpers.utils import (
    _extract_table_references as _extract_table_references,
)

# --- Public symbols (production code) ---
from rivet_core.executor.models import CheckExecutionResult as CheckExecutionResult
from rivet_core.executor.models import ExecutionContext as ExecutionContext
from rivet_core.executor.models import ExecutionResult as ExecutionResult
from rivet_core.executor.models import ExecutionState as ExecutionState
from rivet_core.executor.models import (
    FusedGroupExecutionResult as FusedGroupExecutionResult,
)
from rivet_core.executor.models import GroupCheckPhaseResult as GroupCheckPhaseResult
from rivet_core.executor.models import (
    GroupEnginePhaseResult as GroupEnginePhaseResult,
)
from rivet_core.executor.models import GroupPostprocessResult as GroupPostprocessResult
from rivet_core.executor.models import (
    JointExecutionResult as JointExecutionResult,
)
from rivet_core.executor.models import ProgressCallback as ProgressCallback

# phases/scheduling.py
from rivet_core.executor.phases.scheduling import DependencyGraph as DependencyGraph
from rivet_core.executor.pipeline import Executor as Executor
