# Re-export all public and test-used symbols for backward compatibility.
#
# Every symbol that was previously importable from ``rivet_core.compiler``
# is re-exported here so that existing ``from rivet_core.compiler import X``
# statements continue to work unchanged.
#
# The ``X as X`` pattern is required by mypy strict mode (no_implicit_reexport)
# to mark these as intentional re-exports.

# --- Level 1: models (dataclasses, type aliases, phase constants) ----------
# --- Level 2: private helpers re-exported for test code -------------------
from rivet_core.compiler.helpers.resolution import (
    _build_checkpoint_fq_name as _build_checkpoint_fq_name,
)
from rivet_core.compiler.helpers.resolution import (
    _build_checkpoint_sources as _build_checkpoint_sources,
)
from rivet_core.compiler.helpers.resolution import (
    _inject_checkpoint_ctes as _inject_checkpoint_ctes,
)
from rivet_core.compiler.helpers.resolution import _prepend_ctes as _prepend_ctes
from rivet_core.compiler.helpers.resolution import _resolve_adapter as _resolve_adapter
from rivet_core.compiler.helpers.resolution import (
    _resolve_checkpoint_cte_body as _resolve_checkpoint_cte_body,
)
from rivet_core.compiler.helpers.resolution import _resolve_engine as _resolve_engine
from rivet_core.compiler.helpers.sql_helpers import _analyze_source_sql as _analyze_source_sql
from rivet_core.compiler.helpers.sql_helpers import _compile_sql_joint as _compile_sql_joint
from rivet_core.compiler.helpers.sql_helpers import (
    _compute_source_transform_schema as _compute_source_transform_schema,
)
from rivet_core.compiler.helpers.sql_helpers import (
    _infer_projection_type as _infer_projection_type,
)
from rivet_core.compiler.helpers.sql_helpers import _infer_sink_schemas as _infer_sink_schemas
from rivet_core.compiler.helpers.sql_helpers import (
    _validate_source_inline_transforms as _validate_source_inline_transforms,
)
from rivet_core.compiler.helpers.validation import _compile_checks as _compile_checks
from rivet_core.compiler.helpers.validation import _compile_joint as _compile_joint
from rivet_core.compiler.helpers.validation import (
    _compile_python_joint as _compile_python_joint,
)
from rivet_core.compiler.helpers.validation import (
    _validate_checkpoint_joint as _validate_checkpoint_joint,
)
from rivet_core.compiler.helpers.validation import _verify_callable as _verify_callable
from rivet_core.compiler.models import PHASE_COMPILE_SQL as PHASE_COMPILE_SQL
from rivet_core.compiler.models import PHASE_ENGINE_BOUNDARIES as PHASE_ENGINE_BOUNDARIES
from rivet_core.compiler.models import PHASE_FINALIZATION as PHASE_FINALIZATION
from rivet_core.compiler.models import PHASE_FUSION as PHASE_FUSION
from rivet_core.compiler.models import PHASE_INTROSPECT_SOURCES as PHASE_INTROSPECT_SOURCES
from rivet_core.compiler.models import PHASE_MATERIALIZATION as PHASE_MATERIALIZATION
from rivet_core.compiler.models import PHASE_OPTIMIZATION as PHASE_OPTIMIZATION
from rivet_core.compiler.models import PHASE_PRUNE_DAG as PHASE_PRUNE_DAG
from rivet_core.compiler.models import PHASE_RESOLVE_METADATA as PHASE_RESOLVE_METADATA
from rivet_core.compiler.models import PHASE_STRATEGY_RESOLUTION as PHASE_STRATEGY_RESOLUTION
from rivet_core.compiler.models import AdapterDecision as AdapterDecision
from rivet_core.compiler.models import CompilationContext as CompilationContext
from rivet_core.compiler.models import CompilationDiagnostics as CompilationDiagnostics
from rivet_core.compiler.models import CompilationStats as CompilationStats
from rivet_core.compiler.models import CompilationWarning as CompilationWarning
from rivet_core.compiler.models import CompiledAdapter as CompiledAdapter
from rivet_core.compiler.models import CompiledAssembly as CompiledAssembly
from rivet_core.compiler.models import CompiledCatalog as CompiledCatalog
from rivet_core.compiler.models import CompiledEngine as CompiledEngine
from rivet_core.compiler.models import CompiledJoint as CompiledJoint
from rivet_core.compiler.models import EngineBoundary as EngineBoundary
from rivet_core.compiler.models import EngineResolutionSource as EngineResolutionSource
from rivet_core.compiler.models import ExecutionWave as ExecutionWave
from rivet_core.compiler.models import FusionStrategyName as FusionStrategyName
from rivet_core.compiler.models import IntrospectionRecord as IntrospectionRecord
from rivet_core.compiler.models import Materialization as Materialization
from rivet_core.compiler.models import MaterializationStrategyName as MaterializationStrategyName
from rivet_core.compiler.models import MaterializationTrigger as MaterializationTrigger
from rivet_core.compiler.models import OptimizationResult as OptimizationResult
from rivet_core.compiler.models import OptimizationStatus as OptimizationStatus
from rivet_core.compiler.models import PluginAnnotation as PluginAnnotation
from rivet_core.compiler.models import ResolvedJointMetadata as ResolvedJointMetadata
from rivet_core.compiler.models import SchemaConfidence as SchemaConfidence
from rivet_core.compiler.models import SourceSQLAnalysis as SourceSQLAnalysis
from rivet_core.compiler.models import SourceStats as SourceStats
from rivet_core.compiler.models import TagMode as TagMode
from rivet_core.compiler.models import logger as logger

# --- Level 3: phase instances ---------------------------------------------
from rivet_core.compiler.phases.phase01_prune import prune_dag_phase as prune_dag_phase
from rivet_core.compiler.phases.phase02_metadata import (
    resolve_metadata_phase as resolve_metadata_phase,
)
from rivet_core.compiler.phases.phase03_introspect import (
    introspect_sources_phase as introspect_sources_phase,
)
from rivet_core.compiler.phases.phase04_compile_sql import compile_sql_phase as compile_sql_phase
from rivet_core.compiler.phases.phase05_fusion import fusion_phase as fusion_phase
from rivet_core.compiler.phases.phase06_optimization import (
    optimization_phase as optimization_phase,
)
from rivet_core.compiler.phases.phase07_strategy import (
    strategy_resolution_phase as strategy_resolution_phase,
)
from rivet_core.compiler.phases.phase08_engine_boundaries import (
    engine_boundary_phase as engine_boundary_phase,
)
from rivet_core.compiler.phases.phase09_materialization import (
    materialization_phase as materialization_phase,
)
from rivet_core.compiler.phases.phase10_finalization import (
    finalization_phase as finalization_phase,
)

# --- Level 4: pipeline, public entry points -------------------------------
from rivet_core.compiler.pipeline import CompilationPipeline as CompilationPipeline
from rivet_core.compiler.pipeline import (
    _build_initial_phase_state as _build_initial_phase_state,
)
from rivet_core.compiler.pipeline import (
    _compute_parallel_execution_plan as _compute_parallel_execution_plan,
)
from rivet_core.compiler.pipeline import compile as compile  # noqa: A004
from rivet_core.compiler.pipeline import compile_until as compile_until

# --- Level 1: state (PhaseState, CompileOptions, CompilerPhase) -----------
from rivet_core.compiler.state import CompileOptions as CompileOptions
from rivet_core.compiler.state import CompilerPhase as CompilerPhase
from rivet_core.compiler.state import PhaseState as PhaseState
