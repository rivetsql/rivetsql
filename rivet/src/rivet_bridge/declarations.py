"""DeclarationGenerator: converts Assembly joints back into declaration files."""

from __future__ import annotations

from rivet_bridge.decomposer import SQLDecomposer
from rivet_bridge.models import FileOutput
from rivet_core import Assembly, Joint
from rivet_core.checks import Assertion

_DIR_MAP = {
    "source": "sources",
    "sink": "sinks",
    "sql": "joints",
    "python": "joints",
}


class DeclarationGenerator:
    """Generates declaration files from an Assembly."""

    def __init__(self) -> None:
        self._decomposer = SQLDecomposer()

    def generate(
        self,
        assembly: Assembly,
        format: str = "yaml",
        format_overrides: dict[str, str] | None = None,
        source_formats: dict[str, str] | None = None,
    ) -> list[FileOutput]:
        """Generate declaration files and quality files from Assembly.

        Returns list[FileOutput] sorted by joint name.
        """
        format_overrides = format_overrides or {}
        source_formats = source_formats or {}
        files: list[FileOutput] = []

        for name in sorted(assembly.joints):
            joint = assembly.joints[name]
            fmt = self._resolve_format(name, format, format_overrides, source_formats)
            directory = _DIR_MAP.get(joint.joint_type, "joints")
            ext = "sql" if fmt == "sql" else "yaml"

            if fmt == "sql":
                content = self._generate_sql(joint)
            else:
                content = self._generate_yaml(joint)

            files.append(FileOutput(
                relative_path=f"{directory}/{name}.{ext}",
                content=content,
                joint_name=name,
            ))

            # Quality check files
            if joint.assertions:
                qf = self._generate_quality_file(joint)
                if qf is not None:
                    files.append(qf)

        return files

    def _resolve_format(
        self,
        joint_name: str,
        default: str,
        overrides: dict[str, str],
        source_formats: dict[str, str],
    ) -> str:
        if joint_name in overrides:
            return overrides[joint_name]
        if joint_name in source_formats:
            return source_formats[joint_name]
        if default:
            return default
        return "yaml"

    def _generate_yaml(self, joint: Joint) -> str:
        lines: list[str] = []
        lines.append(f"name: {joint.name}")
        lines.append(f"type: {joint.joint_type}")

        if joint.catalog is not None:
            lines.append(f"catalog: {joint.catalog}")

        if joint.table is not None:
            lines.append(f"table: {joint.table}")

        if joint.engine is not None:
            lines.append(f"engine: {joint.engine}")

        if joint.eager:
            lines.append("eager: true")

        if joint.description is not None:
            lines.append(f"description: {joint.description}")

        # Upstream always emitted
        lines.append(f"upstream: {self._yaml_list(joint.upstream)}")

        if joint.tags:
            lines.append(f"tags: {self._yaml_list(joint.tags)}")

        if joint.write_strategy is not None:
            lines.append("write_strategy:")
            lines.append(f"  mode: {joint.write_strategy}")

        if joint.fusion_strategy_override is not None:
            lines.append(f"fusion_strategy: {joint.fusion_strategy_override}")

        if joint.materialization_strategy_override is not None:
            lines.append(f"materialization_strategy: {joint.materialization_strategy_override}")

        if joint.function is not None:
            lines.append(f"function: {joint.function}")
        elif joint.sql is not None:
            # Try to decompose SQL for source/sink
            if joint.joint_type in ("source", "sink") and self._decomposer.can_decompose(joint.sql):
                columns, filter_str, _table = self._decomposer.decompose(joint.sql)
                if columns is not None:
                    lines.append("columns:")
                    for col in columns:
                        if col.expression is None:
                            lines.append(f"  - name: {col.name}")
                        else:
                            lines.append(f"  - name: {col.name}")
                            lines.append(f"    expression: {col.expression}")
                if filter_str is not None:
                    lines.append(f"filter: {filter_str}")
            else:
                lines.append(f"sql: {joint.sql}")

        lines.append("")
        return "\n".join(lines)

    def _generate_sql(self, joint: Joint) -> str:
        lines: list[str] = []
        lines.append(f"-- rivet:name: {joint.name}")
        lines.append(f"-- rivet:type: {joint.joint_type}")

        if joint.catalog is not None:
            lines.append(f"-- rivet:catalog: {joint.catalog}")

        if joint.table is not None:
            lines.append(f"-- rivet:table: {joint.table}")

        if joint.engine is not None:
            lines.append(f"-- rivet:engine: {joint.engine}")

        if joint.eager:
            lines.append("-- rivet:eager: true")

        if joint.description is not None:
            lines.append(f"-- rivet:description: {joint.description}")

        # Upstream always emitted
        lines.append(f"-- rivet:upstream: {self._sql_list(joint.upstream)}")

        if joint.tags:
            lines.append(f"-- rivet:tags: {self._sql_list(joint.tags)}")

        if joint.write_strategy is not None:
            lines.append(f"-- rivet:write_strategy: {{mode: {joint.write_strategy}}}")

        if joint.fusion_strategy_override is not None:
            lines.append(f"-- rivet:fusion_strategy: {joint.fusion_strategy_override}")

        if joint.materialization_strategy_override is not None:
            lines.append(f"-- rivet:materialization_strategy: {joint.materialization_strategy_override}")

        if joint.function is not None:
            lines.append(f"-- rivet:function: {joint.function}")

        # SQL body
        if joint.sql is not None:
            lines.append(joint.sql)
        lines.append("")
        return "\n".join(lines)

    def _generate_quality_file(self, joint: Joint) -> FileOutput | None:
        assertions: list[Assertion] = [a for a in joint.assertions if a.phase == "assertion"]
        audits: list[Assertion] = [a for a in joint.assertions if a.phase == "audit"]

        if not assertions and not audits:
            return None

        if joint.joint_type == "sink":
            content = self._quality_section_format(assertions, audits)
        else:
            content = self._quality_flat_format(assertions)

        return FileOutput(
            relative_path=f"quality/{joint.name}.yaml",
            content=content,
            joint_name=joint.name,
        )

    def _quality_section_format(
        self,
        assertions: list[Assertion],
        audits: list[Assertion],
    ) -> str:
        lines: list[str] = []
        if assertions:
            lines.append("assertions:")
            for a in assertions:
                lines.extend(self._format_check(a, indent=2))
        if audits:
            lines.append("audits:")
            for a in audits:
                lines.extend(self._format_check(a, indent=2))
        lines.append("")
        return "\n".join(lines)

    def _quality_flat_format(self, assertions: list[Assertion]) -> str:
        lines: list[str] = []
        for a in assertions:
            lines.extend(self._format_check(a, indent=0))
        lines.append("")
        return "\n".join(lines)

    def _format_check(self, check: Assertion, indent: int) -> list[str]:
        prefix = " " * indent
        lines = [f"{prefix}- type: {check.type}"]
        if check.severity != "error":
            lines.append(f"{prefix}  severity: {check.severity}")
        if check.config:
            lines.append(f"{prefix}  config:")
            for k, v in sorted(check.config.items()):
                lines.append(f"{prefix}    {k}: {self._yaml_value(v)}")
        return lines

    @staticmethod
    def _yaml_list(items: list[str]) -> str:
        if not items:
            return "[]"
        return "[" + ", ".join(items) + "]"

    @staticmethod
    def _sql_list(items: list[str]) -> str:
        if not items:
            return "[]"
        return "[" + ", ".join(items) + "]"

    @staticmethod
    def _yaml_value(v) -> str:  # type: ignore[no-untyped-def]
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, list):
            return "[" + ", ".join(str(i) for i in v) + "]"
        return str(v)
