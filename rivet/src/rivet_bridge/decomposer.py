"""SQL decomposition: extracts columns, filter, and table from simple SELECT statements."""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from rivet_config import ColumnDecl


class SQLDecomposer:
    """Decomposes simple SQL back into columns and filter fields for YAML output."""

    def can_decompose(self, sql: str) -> bool:
        """Return True if SQL is a simple SELECT <cols> FROM <single_table> [WHERE <cond>]."""
        try:
            parsed = sqlglot.parse_one(sql)
        except sqlglot.errors.ParseError:
            return False

        if not isinstance(parsed, exp.Select):
            return False

        # Reject UNION / INTERSECT / EXCEPT (parsed as set operations wrapping selects)
        if parsed.find(exp.Union):
            return False

        # Reject CTEs
        if parsed.find(exp.With):
            return False

        # Reject JOINs
        if parsed.find(exp.Join):
            return False

        # Reject subqueries
        if parsed.find(exp.Subquery):
            return False

        # Reject window functions
        if parsed.find(exp.Window):
            return False

        # Reject GROUP BY
        if parsed.args.get("group"):
            return False

        # Reject HAVING
        if parsed.args.get("having"):
            return False

        # Reject ORDER BY
        if parsed.args.get("order"):
            return False

        # Reject DISTINCT
        if parsed.args.get("distinct"):
            return False

        # Must have exactly one FROM table (no multiple tables)
        from_clause = parsed.find(exp.From)
        if from_clause is None:
            return False

        tables = list(from_clause.find_all(exp.Table))
        if len(tables) != 1:
            return False

        return True

    def decompose(self, sql: str) -> tuple[list[ColumnDecl] | None, str | None, str, int | None]:
        """Extract columns, filter, table reference, and limit from simple SQL.

        Returns (columns_or_none, filter_or_none, table_name, limit_or_none).
        columns is None when SELECT *.
        """
        parsed = sqlglot.parse_one(sql)

        # Extract table name
        table = parsed.find(exp.Table)
        table_name = (
            table.name if table.args.get("db") is None else f"{table.args['db'].name}.{table.name}"
        )  # type: ignore[union-attr]

        # Extract columns
        columns: list[ColumnDecl] | None = None
        select_expressions = parsed.args.get("expressions", [])

        if len(select_expressions) == 1 and isinstance(select_expressions[0], exp.Star):
            columns = None
        else:
            columns = []
            for col_expr in select_expressions:
                if isinstance(col_expr, exp.Alias):
                    columns.append(
                        ColumnDecl(
                            name=col_expr.alias,
                            expression=col_expr.this.sql(),
                        )
                    )
                elif isinstance(col_expr, exp.Column):
                    columns.append(ColumnDecl(name=col_expr.name, expression=None))
                else:
                    # Expression without alias — use the SQL as both name and expression
                    columns.append(
                        ColumnDecl(
                            name=col_expr.sql(),
                            expression=col_expr.sql(),
                        )
                    )

        # Extract WHERE filter
        where = parsed.find(exp.Where)
        filter_str = where.this.sql() if where else None

        # Extract LIMIT
        limit_node = parsed.args.get("limit")
        limit_value: int | None = None
        if limit_node is not None:
            limit_expr = limit_node.expression
            if isinstance(limit_expr, exp.Literal) and limit_expr.is_int:
                limit_value = int(limit_expr.this)

        return columns, filter_str, table_name, limit_value
