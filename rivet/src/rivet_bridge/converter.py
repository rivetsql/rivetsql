"""JointConverter: translates JointDeclaration → core Joint."""

from __future__ import annotations

from rivet_bridge.errors import BridgeError
from rivet_config import JointDeclaration
from rivet_core import ComputeEngine, Joint
from rivet_core.checks import Assertion


class JointConverter:
    """Converts a JointDeclaration into a core Joint object."""

    def convert(
        self,
        declaration: JointDeclaration,
        engines_map: dict[str, ComputeEngine],
    ) -> tuple[Joint | None, list[BridgeError]]:
        errors: list[BridgeError] = []
        src = str(declaration.source_path)

        # Validate engine reference
        if declaration.engine is not None and declaration.engine not in engines_map:
            errors.append(
                BridgeError(
                    code="BRG-207",
                    message=f"Joint '{declaration.name}' references unknown engine '{declaration.engine}'.",
                    joint_name=declaration.name,
                    source_file=src,
                    remediation=f"Add engine '{declaration.engine}' to the profile or remove the engine override.",
                )
            )

        # Validate audit on non-sink
        for qc in declaration.quality_checks:
            if qc.phase == "audit" and declaration.joint_type != "sink":
                errors.append(
                    BridgeError(
                        code="BRG-206",
                        message=f"Joint '{declaration.name}' has audit quality check but is not a sink.",
                        joint_name=declaration.name,
                        source_file=src,
                        remediation="Move audit checks to a sink joint or change phase to 'assertion'.",
                    )
                )
                break  # one error per joint is sufficient

        if errors:
            return None, errors

        # Convert assertion quality checks
        assertions = [
            Assertion(
                type=qc.check_type,
                severity=qc.severity,
                config=qc.config,
                phase=qc.phase,
            )
            for qc in declaration.quality_checks
            if qc.phase == "assertion"
        ]

        joint = Joint(
            name=declaration.name,
            joint_type=declaration.joint_type,
            catalog=declaration.catalog,
            upstream=declaration.upstream if declaration.upstream is not None else [],
            tags=declaration.tags if declaration.tags is not None else [],
            description=declaration.description,
            assertions=assertions,
            sql=declaration.sql,
            engine=declaration.engine,
            eager=declaration.eager,
            table=declaration.table,
            write_strategy=declaration.write_strategy.mode if declaration.write_strategy else None,
            function=declaration.function,
            source_file=str(declaration.source_path),
            fusion_strategy_override=declaration.fusion_strategy,
            materialization_strategy_override=declaration.materialization_strategy,
        )

        return joint, []
