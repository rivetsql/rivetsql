"""YAML joint declaration parser."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from rivet_config.errors import ConfigError
from rivet_config.models import (
    JOINT_NAME_MAX_LENGTH,
    JOINT_NAME_PATTERN,
    JOINT_TYPES,
    WRITE_STRATEGY_MODES,
    YAML_JOINT_FIELDS,
    ColumnDecl,
    JointDeclaration,
    WriteStrategyDecl,
)

# Type-specific required fields.
_TYPE_REQUIRED: dict[str, list[str]] = {
    "source": ["catalog"],
    "sink": ["catalog", "table"],
    "sql": ["sql"],
    "python": ["function"],
}


class YAMLParser:
    """Parses a single YAML joint declaration file."""

    def parse(self, file_path: Path) -> tuple[JointDeclaration | None, list[ConfigError]]:
        errors: list[ConfigError] = []

        # Read and parse YAML.
        try:
            raw = yaml.safe_load(file_path.read_text())
        except (OSError, yaml.YAMLError) as exc:
            errors.append(ConfigError(
                source_file=file_path,
                message=f"Failed to read or parse YAML: {exc}",
                remediation="Ensure the file exists and contains valid YAML.",
            ))
            return None, errors

        if not isinstance(raw, dict):
            errors.append(ConfigError(
                source_file=file_path,
                message="YAML file must contain a mapping at the top level.",
                remediation="Ensure the file is a YAML mapping (key: value pairs).",
            ))
            return None, errors

        # Reject unrecognized keys.
        unrecognized = set(raw) - YAML_JOINT_FIELDS
        if unrecognized:
            errors.append(ConfigError(
                source_file=file_path,
                message=f"Unrecognized keys: {sorted(unrecognized)}",
                remediation=f"Remove or rename unrecognized keys. Recognized keys: {sorted(YAML_JOINT_FIELDS)}",
            ))

        # Validate name.
        name = raw.get("name")
        if name is None:
            errors.append(ConfigError(
                source_file=file_path,
                message="Missing required field 'name'.",
                remediation="Add a 'name' field to the joint declaration.",
            ))
        else:
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

        # Validate type.
        joint_type = raw.get("type")
        if joint_type is None:
            errors.append(ConfigError(
                source_file=file_path,
                message="Missing required field 'type'.",
                remediation=f"Add a 'type' field. Valid types: {sorted(JOINT_TYPES)}",
            ))
        elif joint_type not in JOINT_TYPES:
            errors.append(ConfigError(
                source_file=file_path,
                message=f"Invalid joint type '{joint_type}'.",
                remediation=f"Use one of: {sorted(JOINT_TYPES)}",
            ))

        # Validate type-specific required fields.
        if joint_type in _TYPE_REQUIRED:
            for req in _TYPE_REQUIRED[joint_type]:
                if req not in raw or raw[req] is None:
                    errors.append(ConfigError(
                        source_file=file_path,
                        message=f"Missing required field '{req}' for type '{joint_type}'.",
                        remediation=f"Add '{req}' to the joint declaration.",
                    ))

        # Parse columns.
        columns = self._parse_columns(raw.get("columns"), file_path, errors)

        # Parse write_strategy.
        write_strategy = self._parse_write_strategy(raw, file_path, errors)

        if errors:
            return None, errors

        return JointDeclaration(
            name=name,  # type: ignore[arg-type]
            joint_type=joint_type,  # type: ignore[arg-type]
            source_path=file_path,
            sql=raw.get("sql"),
            catalog=raw.get("catalog"),
            table=raw.get("table"),
            columns=columns,
            filter=raw.get("filter"),
            write_strategy=write_strategy,
            function=raw.get("function"),
            engine=raw.get("engine"),
            eager=bool(raw.get("eager", False)),
            upstream=raw.get("upstream"),
            tags=raw.get("tags"),
            description=raw.get("description"),
            source_format="yaml",
            fusion_strategy=raw.get("fusion_strategy"),
            materialization_strategy=raw.get("materialization_strategy"),
        ), errors

    def _parse_columns(
        self,
        raw_columns: Any,
        file_path: Path,
        errors: list[ConfigError],
    ) -> list[ColumnDecl] | None:
        if raw_columns is None:
            return None
        if not isinstance(raw_columns, list):
            errors.append(ConfigError(
                source_file=file_path,
                message="'columns' must be a list.",
                remediation="Provide columns as a YAML list.",
            ))
            return None
        if len(raw_columns) == 0:
            errors.append(ConfigError(
                source_file=file_path,
                message="'columns' must not be empty. Omit 'columns' for SELECT *.",
                remediation="Remove the empty 'columns' list or add column entries.",
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
                    message=f"Invalid column entry: {entry!r}. Must be a string or single-key mapping.",
                    remediation="Use 'column_name' or 'alias: expression' format.",
                ))
        return result

    def _parse_write_strategy(
        self,
        raw: dict[str, Any],
        file_path: Path,
        errors: list[ConfigError],
    ) -> WriteStrategyDecl | None:
        ws = raw.get("write_strategy")
        joint_type = raw.get("type")

        if ws is None:
            if joint_type == "sink":
                return WriteStrategyDecl(mode="append", options={})
            return None

        if not isinstance(ws, dict):
            errors.append(ConfigError(
                source_file=file_path,
                message="'write_strategy' must be a mapping.",
                remediation="Provide write_strategy as a YAML mapping with at least a 'mode' key.",
            ))
            return None

        mode = ws.get("mode")
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

        options = {k: v for k, v in ws.items() if k != "mode"}
        return WriteStrategyDecl(mode=mode, options=options)
