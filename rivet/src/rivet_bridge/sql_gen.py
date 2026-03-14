"""SQL generation from YAML columns/filter fields using sqlglot."""

from __future__ import annotations

import sqlglot
from sqlglot import expressions as exp

from rivet_bridge.errors import BridgeError
from rivet_config import JointDeclaration


class SQLGenerator:
    """Generates SQL queries from YAML source/sink columns and filter fields."""

    def generate(
        self,
        declaration: JointDeclaration,
        joint_names: set[str],
    ) -> tuple[str, list[BridgeError]]:
        """Generate SQL from YAML columns/filter fields.

        Returns (sql_string, errors).
        """
        try:
            # Determine FROM reference
            if declaration.joint_type == "source":
                table_ref = declaration.table or declaration.name
            elif declaration.joint_type == "sink":
                table_ref = declaration.upstream[0] if declaration.upstream else declaration.name
            else:
                table_ref = declaration.table or declaration.name

            # Build columns
            if declaration.columns is None:
                select_cols: list[exp.Expression] = [exp.Star()]
            else:
                select_cols = []
                for col in declaration.columns:
                    if col.expression is None:
                        select_cols.append(exp.Column(this=exp.to_identifier(col.name)))
                    else:
                        select_cols.append(
                            exp.Alias(
                                this=sqlglot.parse_one(col.expression),
                                alias=exp.to_identifier(col.name),
                            )
                        )

            select = (
                exp.Select()
                .select(*select_cols)
                .from_(exp.Table(this=exp.to_identifier(table_ref)))
            )

            if declaration.filter:
                select = select.where(sqlglot.parse_one(declaration.filter))

            if declaration.limit is not None:
                select = select.limit(declaration.limit)

            return select.sql(), []

        except Exception as exc:
            return "", [
                BridgeError(
                    code="BRG-101",
                    message=f"SQL generation failed for joint '{declaration.name}': {exc}",
                    joint_name=declaration.name,
                    source_file=str(declaration.source_path),
                    remediation="Check columns and filter syntax in the declaration.",
                )
            ]
