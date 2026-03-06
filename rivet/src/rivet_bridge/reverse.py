"""Reverse path orchestrator: Assembly → project scaffold."""

from __future__ import annotations

from pathlib import Path

from rivet_bridge.declarations import DeclarationGenerator
from rivet_bridge.errors import BridgeError, BridgeValidationError
from rivet_bridge.models import BridgeResult, FileOutput, ProjectOutput
from rivet_bridge.profiles import ProfileGenerator


def generate_project(
    bridge_result: BridgeResult,
    format: str = "yaml",
    output_dir: Path | None = None,
    overwrite: bool = False,
) -> ProjectOutput:
    """Generate a complete project scaffold from BridgeResult.

    Raises BridgeValidationError with BRG-401 if output_dir has files and overwrite is False.
    """
    if output_dir is None:
        output_dir = Path.cwd() / "output"

    # Check output directory
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise BridgeValidationError([
            BridgeError(
                code="BRG-401",
                message=f"Output directory '{output_dir}' is not empty.",
                remediation="Use overwrite=True or choose an empty directory.",
            )
        ])

    # Generate rivet.yaml manifest
    rivet_yaml = FileOutput(
        relative_path="rivet.yaml",
        content=(
            "profiles: profiles.yaml\n"
            "sources: sources/\n"
            "joints: joints/\n"
            "sinks: sinks/\n"
            "quality: quality/\n"
        ),
        joint_name=None,
    )

    # Generate declarations and quality files
    all_files = DeclarationGenerator().generate(
        assembly=bridge_result.assembly,
        format=format,
        source_formats=bridge_result.source_formats,
    )
    declarations = [f for f in all_files if not f.relative_path.startswith("quality/")]
    quality_files = [f for f in all_files if f.relative_path.startswith("quality/")]

    # Generate profile
    profile = ProfileGenerator().generate(bridge_result.profile_snapshot)

    # Write all files
    all_outputs = [rivet_yaml, profile] + declarations + quality_files
    for file_output in all_outputs:
        path = output_dir / file_output.relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(file_output.content)

    return ProjectOutput(
        rivet_yaml=rivet_yaml,
        profile=profile,
        declarations=declarations,
        quality_files=quality_files,
        output_dir=output_dir,
    )
