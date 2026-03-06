"""Visual format renderer for compile output."""

from __future__ import annotations

from rivet_cli.rendering.formatter import AssemblyFormatter
from rivet_core.compiler import CompiledAssembly


def render_visual(compiled: CompiledAssembly, verbosity: int, color: bool) -> str:
    """Render CompiledAssembly as structured text diagram."""
    return AssemblyFormatter(color=color, verbosity=verbosity).render(compiled)
