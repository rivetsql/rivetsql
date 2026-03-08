"""JSON format renderer for compile and run output."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from rivet_core.compiler import CompiledAssembly
from rivet_core.executor import ExecutionResult


def _default(obj: Any) -> Any:
    """Custom JSON serializer for non-JSON-native types."""
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, (set, frozenset)):
        return sorted(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def render_compile_json(compiled: CompiledAssembly) -> str:
    """Serialize CompiledAssembly to JSON."""
    return json.dumps(asdict(compiled), indent=2, default=_default)


def render_run_json(result: ExecutionResult, compiled: CompiledAssembly) -> str:
    """Serialize ExecutionResult to JSON."""
    data: dict[str, Any] = {
        "execution": asdict(result),
        "compilation": asdict(compiled),
    }
    if result.run_stats is not None:
        data["run_stats"] = result.run_stats.to_dict()
    return json.dumps(data, indent=2, default=_default)
