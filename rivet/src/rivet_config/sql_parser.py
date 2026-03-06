"""SQL joint declaration parser: annotations + SQL body → JointDeclaration."""

from __future__ import annotations

from pathlib import Path

from rivet_config.annotations import AnnotationParser
from rivet_config.errors import ConfigError
from rivet_config.models import (
    JOINT_NAME_MAX_LENGTH,
    JOINT_NAME_PATTERN,
    JOINT_TYPES,
    WRITE_STRATEGY_MODES,
    ColumnDecl,
    JointDeclaration,
    WriteStrategyDecl,
)

# Annotation keys that map to JointDeclaration fields.
_RECOGNIZED_KEYS = frozenset({
    "name", "type", "catalog", "table", "columns", "filter", "engine",
    "eager", "upstream", "tags", "description", "write_strategy",
    "function", "fusion_strategy", "materialization_strategy",
})

# Quality-related annotation prefixes handled by QualityParser, not here.
_QUALITY_PREFIXES = ("assert", "audit", "quality.")

# Type-specific required fields (same as YAMLParser).
_TYPE_REQUIRED: dict[str, list[str]] = {
    "source": ["catalog"],
    "sink": ["catalog", "table"],
    "sql": ["sql"],
    "python": ["function"],
}


class SQLParser:
    """Parses a single SQL joint declaration file."""

    def __init__(self) -> None:
        self._annotation_parser = AnnotationParser()

    def parse(self, file_path: Path) -> tuple[JointDeclaration | None, list[ConfigError]]:
        errors: list[ConfigError] = []

        try:
            text = file_path.read_text()
        except OSError as exc:
            errors.append(ConfigError(
                source_file=file_path,
                message=f"Failed to read SQL file: {exc}",
                remediation="Ensure the file exists and is readable.",
            ))
            return None, errors

        lines = text.splitlines(keepends=True)
        annotations, first_sql_line, parse_errors = self._annotation_parser.parse(lines, file_path)
        errors.extend(parse_errors)

        # Build field mapping from annotations.
        fields: dict[str, object] = {}
        for ann in annotations:
            key = ann.key
            # Skip quality annotations — handled by QualityParser.
            if any(key.startswith(p) for p in _QUALITY_PREFIXES):
                continue
            if key not in _RECOGNIZED_KEYS:
                errors.append(ConfigError(
                    source_file=file_path,
                    message=f"Unrecognized annotation key '{key}'.",
                    remediation=f"Remove or rename. Recognized keys: {sorted(_RECOGNIZED_KEYS)}",
                    line_number=ann.line_number,
                ))
                continue
            fields[key] = ann.value

        # Extract SQL body.
        sql_body = "".join(lines[first_sql_line:]).strip() or None

        # Default type to 'sql' if not declared.
        joint_type = str(fields.get("type", "sql"))

        # Validate type.
        if joint_type not in JOINT_TYPES:
            errors.append(ConfigError(
                source_file=file_path,
                message=f"Invalid joint type '{joint_type}'.",
                remediation=f"Use one of: {sorted(JOINT_TYPES)}",
            ))

        # Validate name.
        name = fields.get("name")
        if name is None:
            # Derive name from filename stem.
            name = file_path.stem
        name = str(name)

        if not JOINT_NAME_PATTERN.match(name):
            errors.append(ConfigError(
                source_file=file_path,
                message=f"Invalid joint name '{name}'. Must match [a-z][a-z0-9_]*.",
                remediation="Use a name starting with a lowercase letter, containing only lowercase letters, digits, and underscores.",
            ))
        if len(name) > JOINT_NAME_MAX_LENGTH:
            errors.append(ConfigError(
                source_file=file_path,
                message=f"Joint name '{name}' exceeds maximum length of {JOINT_NAME_MAX_LENGTH}.",
                remediation=f"Shorten the name to at most {JOINT_NAME_MAX_LENGTH} characters.",
            ))

        # For sql type, the SQL body serves as the required 'sql' field.
        # Validate type-specific required fields.
        if joint_type in _TYPE_REQUIRED:
            for req in _TYPE_REQUIRED[joint_type]:
                if req == "sql":
                    if sql_body is None:
                        errors.append(ConfigError(
                            source_file=file_path,
                            message=f"Missing required SQL body for type '{joint_type}'.",
                            remediation="Add SQL content after the annotations.",
                        ))
                elif req not in fields or fields[req] is None:
                    errors.append(ConfigError(
                        source_file=file_path,
                        message=f"Missing required field '{req}' for type '{joint_type}'.",
                        remediation=f"Add '-- rivet:{req}: <value>' annotation.",
                    ))

        if errors:
            return None, errors

        # Parse columns.
        columns = self._parse_columns(fields.get("columns"), file_path, errors)

        # Parse write_strategy.
        write_strategy = self._parse_write_strategy(fields.get("write_strategy"), joint_type, file_path, errors)

        if errors:
            return None, errors

        return JointDeclaration(
            name=name,
            joint_type=joint_type,
            source_path=file_path,
            sql=sql_body,
            catalog=str(fields["catalog"]) if "catalog" in fields else None,
            table=str(fields["table"]) if "table" in fields else None,
            columns=columns,
            filter=str(fields["filter"]) if "filter" in fields else None,
            write_strategy=write_strategy,
            function=str(fields["function"]) if "function" in fields else None,
            engine=str(fields["engine"]) if "engine" in fields else None,
            eager=bool(fields.get("eager", False)),
            upstream=fields.get("upstream") if "upstream" in fields else None,  # type: ignore[arg-type]
            tags=fields.get("tags") if "tags" in fields else None,  # type: ignore[arg-type]
            description=str(fields["description"]) if "description" in fields else None,
            source_format="sql",
            fusion_strategy=str(fields["fusion_strategy"]) if "fusion_strategy" in fields else None,
            materialization_strategy=str(fields["materialization_strategy"]) if "materialization_strategy" in fields else None,
        ), errors

    def _parse_columns(
        self,
        raw_columns: object,
        file_path: Path,
        errors: list[ConfigError],
    ) -> list[ColumnDecl] | None:
        if raw_columns is None:
            return None
        if not isinstance(raw_columns, list):
            errors.append(ConfigError(
                source_file=file_path,
                message="'columns' annotation must be a list.",
                remediation="Use bracket syntax: -- rivet:columns: [col_a, col_b]",
            ))
            return None
        if len(raw_columns) == 0:
            errors.append(ConfigError(
                source_file=file_path,
                message="'columns' must not be empty. Omit for SELECT *.",
                remediation="Remove the empty columns annotation or add column entries.",
            ))
            return None
        result: list[ColumnDecl] = []
        for entry in raw_columns:
            if isinstance(entry, str):
                result.append(ColumnDecl(name=entry, expression=None))
            elif isinstance(entry, dict) and len(entry) == 1:
                col_name, expr = next(iter(entry.items()))
                result.append(ColumnDecl(name=str(col_name), expression=str(expr)))
            else:
                errors.append(ConfigError(
                    source_file=file_path,
                    message=f"Invalid column entry: {entry!r}.",
                    remediation="Use 'column_name' or 'alias: expression' format.",
                ))
        return result

    def _parse_write_strategy(
        self,
        raw_ws: object,
        joint_type: str,
        file_path: Path,
        errors: list[ConfigError],
    ) -> WriteStrategyDecl | None:
        if raw_ws is None:
            if joint_type == "sink":
                return WriteStrategyDecl(mode="append", options={})
            return None
        if not isinstance(raw_ws, dict):
            errors.append(ConfigError(
                source_file=file_path,
                message="'write_strategy' must be a mapping.",
                remediation="Use: -- rivet:write_strategy: {mode: append}",
            ))
            return None
        mode = raw_ws.get("mode")
        if mode is None:
            errors.append(ConfigError(
                source_file=file_path,
                message="'write_strategy' is missing required 'mode' field.",
                remediation=f"Add a 'mode' field. Valid modes: {sorted(WRITE_STRATEGY_MODES)}",
            ))
            return None
        if mode not in WRITE_STRATEGY_MODES:
            errors.append(ConfigError(
                source_file=file_path,
                message=f"Invalid write strategy mode '{mode}'.",
                remediation=f"Use one of: {sorted(WRITE_STRATEGY_MODES)}",
            ))
            return None
        options = {k: v for k, v in raw_ws.items() if k != "mode"}
        return WriteStrategyDecl(mode=mode, options=options)
