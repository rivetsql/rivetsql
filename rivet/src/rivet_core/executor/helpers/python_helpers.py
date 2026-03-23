"""Python joint normalization functions for the executor.

Functions that normalize Python joint results and create error materials.
"""

from __future__ import annotations

from typing import Any

import pyarrow

from rivet_core.errors import ExecutionError, RivetError
from rivet_core.models import Material
from rivet_core.strategies import MaterializedRef, _ArrowMaterializedRef


def _normalize_python_result(joint_name: str, func_path: str, result: Any) -> Material:
    """Normalize a PythonJoint return value to a Material.

    Accepts: Material, MaterializedRef, pyarrow.Table, pandas.DataFrame,
    polars.DataFrame, pyspark.sql.DataFrame.
    Raises RVT-752 on None or unsupported type.
    """
    _remediation = (
        "Return a Material, MaterializedRef, pyarrow.Table, pandas.DataFrame, "
        "polars.DataFrame, or pyspark DataFrame."
    )

    if result is None:
        raise ExecutionError(
            RivetError(
                code="RVT-752",
                message=f"PythonJoint '{joint_name}' returned None.",
                context={"joint": joint_name, "function": func_path, "return_type": "NoneType"},
                remediation=_remediation,
            )
        )

    # Material passthrough
    if isinstance(result, Material):
        if result.materialized_ref is not None:
            return result
        raise ExecutionError(
            RivetError(
                code="RVT-752",
                message=f"PythonJoint '{joint_name}' returned a Material with no MaterializedRef.",
                context={"joint": joint_name, "function": func_path, "return_type": "Material"},
                remediation=_remediation,
            )
        )

    # MaterializedRef wrapping
    if isinstance(result, MaterializedRef):
        return Material(name=joint_name, catalog="", materialized_ref=result, state="materialized")

    # DataFrame branches — convert to Arrow then wrap
    table: pyarrow.Table | None = None

    if isinstance(result, pyarrow.Table):
        table = result
    else:
        # pandas DataFrame
        try:
            import pandas

            if isinstance(result, pandas.DataFrame):
                table = pyarrow.Table.from_pandas(result)
        except ImportError:
            pass

        # polars DataFrame
        if table is None:
            try:
                import polars

                if isinstance(result, polars.DataFrame):
                    table = result.to_arrow()
            except ImportError:
                pass

        # pyspark DataFrame
        if table is None:
            try:
                import pyspark.sql

                if isinstance(result, pyspark.sql.DataFrame):
                    table = pyarrow.Table.from_pandas(result.toPandas())
            except ImportError:
                pass

    if table is not None:
        ref = _ArrowMaterializedRef(table)
        return Material(name=joint_name, catalog="", materialized_ref=ref, state="materialized")

    raise ExecutionError(
        RivetError(
            code="RVT-752",
            message=f"PythonJoint '{joint_name}' returned unsupported type '{type(result).__name__}'.",
            context={
                "joint": joint_name,
                "function": func_path,
                "return_type": type(result).__name__,
            },
            remediation=_remediation,
        )
    )


# ---------------------------------------------------------------------------
# ErrorMaterial — placeholder for failed joint outputs (Req 40.2)
# ---------------------------------------------------------------------------


def _make_error_material(joint_name: str, error: RivetError) -> Material:
    """Create an ErrorMaterial for a failed joint's output."""
    return Material(
        name=joint_name,
        catalog="",
        state="error",
        schema=None,
    )
