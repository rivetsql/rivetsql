"""Fixture functions for PythonJoint execution tests."""
from __future__ import annotations

from typing import Any

import pyarrow

from rivet_core.context import RivetContext


def transform_arrow(material: Any) -> pyarrow.Table:
    """Simple transform returning an Arrow table."""
    table = material.to_arrow()
    return table.append_column("added", pyarrow.array([1] * table.num_rows, type=pyarrow.int64()))


def transform_multi(inputs: dict[str, Any]) -> pyarrow.Table:
    """Multi-input transform merging two tables."""
    tables = [m.to_arrow() for m in inputs.values()]
    if not tables:
        return pyarrow.table({})
    return pyarrow.concat_tables(tables)


def transform_returns_none(material: Any) -> None:
    """Returns None — should trigger RVT-752."""
    return None


def transform_raises(material: Any) -> pyarrow.Table:
    """Raises an exception — should trigger RVT-751."""
    raise ValueError("intentional error")


def transform_returns_string(material: Any) -> str:  # type: ignore[return]
    """Returns unsupported type — should trigger RVT-752."""
    return "not a table"


def transform_with_context(material: Any, context: RivetContext | None = None) -> pyarrow.Table:
    """Accepts optional RivetContext — verifies it's actually passed."""
    if context is None:
        raise ValueError("Expected RivetContext but got None")
    table = material.to_arrow()
    return table.append_column("ctx", pyarrow.array([context.joint_name] * table.num_rows, type=pyarrow.utf8()))


async def transform_async(material: Any) -> pyarrow.Table:
    """Async transform."""
    return material.to_arrow()
