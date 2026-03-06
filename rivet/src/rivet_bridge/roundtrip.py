"""Roundtrip verification: compare two sets of JointDeclarations for semantic equivalence."""

from __future__ import annotations

import re

from rivet_bridge.models import RoundtripDifference, RoundtripResult
from rivet_config import JointDeclaration


def _normalize_sql(sql: str | None) -> str | None:
    """Normalize SQL by collapsing whitespace for comparison."""
    if sql is None:
        return None
    return re.sub(r"\s+", " ", sql).strip()


def _sorted_or_none(lst: list[str] | None) -> list[str] | None:
    if lst is None:
        return None
    return sorted(lst)


class RoundtripVerifier:
    """Compare two sets of JointDeclarations for semantic equivalence."""

    def verify_roundtrip(
        self,
        original: list[JointDeclaration],
        generated: list[JointDeclaration],
    ) -> RoundtripResult:
        differences: list[RoundtripDifference] = []

        orig_map = {d.name: d for d in original}
        gen_map = {d.name: d for d in generated}

        # Check for missing/extra joints
        for name in sorted(orig_map.keys() - gen_map.keys()):
            differences.append(RoundtripDifference(name, "name", f"joint '{name}': missing in generated"))
        for name in sorted(gen_map.keys() - orig_map.keys()):
            differences.append(RoundtripDifference(name, "name", f"joint '{name}': unexpected in generated"))

        # Compare shared joints
        for name in sorted(orig_map.keys() & gen_map.keys()):
            differences.extend(self._compare_joint(orig_map[name], gen_map[name]))

        return RoundtripResult(equivalent=len(differences) == 0, differences=differences)

    def _compare_joint(
        self, orig: JointDeclaration, gen: JointDeclaration
    ) -> list[RoundtripDifference]:
        diffs: list[RoundtripDifference] = []
        name = orig.name

        # Simple field comparisons
        simple_fields: list[tuple[str, object, object]] = [
            ("joint_type", orig.joint_type, gen.joint_type),
            ("catalog", orig.catalog, gen.catalog),
            ("engine", orig.engine, gen.engine),
            ("table", orig.table, gen.table),
            ("eager", orig.eager, gen.eager),
            ("description", orig.description, gen.description),
            ("function", orig.function, gen.function),
            ("fusion_strategy", orig.fusion_strategy, gen.fusion_strategy),
            ("materialization_strategy", orig.materialization_strategy, gen.materialization_strategy),
        ]
        for field_name, orig_val, gen_val in simple_fields:
            if orig_val != gen_val:
                diffs.append(RoundtripDifference(name, field_name, f"joint '{name}': {field_name} differs"))

        # SQL: whitespace-normalized comparison
        if _normalize_sql(orig.sql) != _normalize_sql(gen.sql):
            diffs.append(RoundtripDifference(name, "sql", f"joint '{name}': sql differs"))

        # Upstream: order-independent
        if _sorted_or_none(orig.upstream) != _sorted_or_none(gen.upstream):
            diffs.append(RoundtripDifference(name, "upstream", f"joint '{name}': upstream differs"))

        # Tags: order-independent
        if _sorted_or_none(orig.tags) != _sorted_or_none(gen.tags):
            diffs.append(RoundtripDifference(name, "tags", f"joint '{name}': tags differs"))

        # Write strategy
        orig_ws = (orig.write_strategy.mode, orig.write_strategy.options) if orig.write_strategy else None
        gen_ws = (gen.write_strategy.mode, gen.write_strategy.options) if gen.write_strategy else None
        if orig_ws != gen_ws:
            diffs.append(RoundtripDifference(name, "write_strategy", f"joint '{name}': write_strategy differs"))

        # Quality checks: compare as sets of (check_type, phase, severity, config)
        orig_qc = {(q.check_type, q.phase, q.severity, tuple(sorted(q.config.items()))) for q in orig.quality_checks}
        gen_qc = {(q.check_type, q.phase, q.severity, tuple(sorted(q.config.items()))) for q in gen.quality_checks}
        if orig_qc != gen_qc:
            diffs.append(RoundtripDifference(name, "quality_checks", f"joint '{name}': quality_checks differs"))

        return diffs
