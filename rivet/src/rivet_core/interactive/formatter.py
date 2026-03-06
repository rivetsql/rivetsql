"""SQL formatter using sqlglot pretty-print.

Requirements: 16.1, 16.3, 16.4, 16.5
"""

from __future__ import annotations

import sqlglot
import sqlglot.errors


class SqlFormatError(Exception):
    """Raised when SQL cannot be parsed for formatting."""

    def __init__(self, message: str, line: int | None = None) -> None:
        self.line = line
        super().__init__(message)


def format_sql(
    sql: str,
    *,
    dialect: str | None = None,
    indent: int = 2,
    uppercase_keywords: bool = True,
    trailing_commas: bool = True,
    max_line_length: int = 80,
) -> str:
    """Format SQL using sqlglot pretty-print.

    Args:
        sql: The SQL string to format.
        dialect: Optional sqlglot dialect name (e.g. "duckdb", "postgres").
        indent: Spaces per indentation level.
        uppercase_keywords: Whether to uppercase SQL keywords.
        trailing_commas: Whether to use trailing commas in SELECT lists.
            sqlglot uses leading_comma internally; trailing_commas=True means
            leading_comma=False.
        max_line_length: Soft wrap target (passed to sqlglot as max_text_width).

    Returns:
        The formatted SQL string.

    Raises:
        SqlFormatError: If the SQL cannot be parsed, with the line number of the
            first error when available.
    """
    if not sql or not sql.strip():
        raise SqlFormatError("SQL parse error at line 1", line=1)

    try:
        statements = sqlglot.parse(sql, dialect=dialect, error_level=sqlglot.ErrorLevel.RAISE)
    except sqlglot.errors.SqlglotError as exc:
        line: int | None = None
        errors = getattr(exc, "errors", None)
        if errors:
            first = errors[0]
            # errors are dicts with a "line" key
            if isinstance(first, dict):
                line = first.get("line")
            else:
                line = getattr(first, "line", None)
        raise SqlFormatError(
            f"SQL parse error at line {line}" if line is not None else "SQL parse error",
            line=line,
        ) from exc

    if not statements:
        raise SqlFormatError("SQL parse error at line 1", line=1)

    formatted_parts: list[str] = []
    for stmt in statements:
        if stmt is None:
            continue
        formatted_parts.append(
            stmt.sql(
                dialect=dialect,
                pretty=True,
                indent=indent,
                normalize=uppercase_keywords,
                pad=indent,
                max_text_width=max_line_length,
                # sqlglot uses leading_comma; trailing_commas=True → leading_comma=False
                leading_comma=not trailing_commas,
            )
        )

    if not formatted_parts:
        raise SqlFormatError("SQL parse error at line 1", line=1)

    return ";\n".join(formatted_parts)
