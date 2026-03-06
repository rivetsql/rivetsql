"""Upstream dependency inference from SQL."""

from __future__ import annotations

import sqlglot
from sqlglot import expressions as exp

from rivet_bridge.errors import BridgeError
from rivet_config import JointDeclaration


class UpstreamInferrer:
    def infer(
        self,
        declaration: JointDeclaration,
        joint_names: set[str],
    ) -> tuple[list[str], list[BridgeError]]:
        # Explicit upstream (including empty list) — use as-is
        if declaration.upstream is not None:
            return declaration.upstream, []

        # Sources never have upstream
        if declaration.joint_type == "source":
            return [], []

        if not declaration.sql:
            return [], []

        errors: list[BridgeError] = []
        try:
            parsed = sqlglot.parse_one(declaration.sql)
            table_refs: list[str] = []
            for table in parsed.find_all(exp.Table):
                name = table.name
                if name and name in joint_names:
                    if name not in table_refs:
                        table_refs.append(name)
            return table_refs, errors
        except Exception as exc:
            errors.append(
                BridgeError(
                    code="BRG-101",
                    message=f"Failed to parse SQL for upstream inference on joint '{declaration.name}': {exc}",
                    joint_name=declaration.name,
                    source_file=str(declaration.source_path),
                    remediation="Check the SQL syntax in your declaration.",
                )
            )
            return [], errors
