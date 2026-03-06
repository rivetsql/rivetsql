"""RivetContext — execution context passed to PythonJoint functions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from rivet_core.logging import RivetLogger


@dataclass(frozen=True)
class RivetContext:
    """Execution context provided to PythonJoint callables.

    Carries joint identity, joint-level options, a scoped logger, and
    arbitrary run-level metadata (e.g. run_id, profile, timestamps).
    """

    joint_name: str
    options: dict[str, Any] = field(default_factory=dict)
    logger: RivetLogger = field(default_factory=RivetLogger)
    run_metadata: dict[str, Any] = field(default_factory=dict)
