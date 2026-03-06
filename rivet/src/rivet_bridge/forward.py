"""Forward path orchestrator: config → Assembly."""

from __future__ import annotations

from dataclasses import replace

from rivet_bridge.builder import AssemblyBuilder
from rivet_bridge.catalogs import CatalogInstantiator
from rivet_bridge.converter import JointConverter
from rivet_bridge.engines import EngineInstantiator
from rivet_bridge.errors import BridgeError, BridgeValidationError
from rivet_bridge.models import BridgeResult
from rivet_bridge.sql_gen import SQLGenerator
from rivet_bridge.upstream import UpstreamInferrer
from rivet_config import ConfigResult, ProjectDeclaration
from rivet_core import PluginRegistry


def build_assembly(
    project: ProjectDeclaration | ConfigResult,
    plugin_registry: PluginRegistry,
) -> BridgeResult:
    """Orchestrate the full forward path: config → Assembly.

    Accepts either a ProjectDeclaration (primary API) or a ConfigResult
    (legacy). Raises BridgeValidationError if any errors are collected.
    """
    if isinstance(project, ConfigResult):
        config_result = project
        if not config_result.success:
            raise BridgeValidationError([
                BridgeError(
                    code="BRG-100",
                    message="Config parsing failed.",
                    remediation="Fix the errors reported by rivet-config.",
                )
            ])
        profile = config_result.profile
        assert profile is not None
        declarations_input = list(config_result.declarations)
    else:
        profile = project.profile
        declarations_input = list(project.joints)

    all_errors: list[BridgeError] = []

    # Step 2: Instantiate catalogs
    catalogs, cat_errors = CatalogInstantiator().instantiate_all(profile, plugin_registry)
    all_errors.extend(cat_errors)

    # Step 3: Instantiate engines
    engines, eng_errors = EngineInstantiator().instantiate_all(profile, plugin_registry)
    all_errors.extend(eng_errors)

    # Step 4: Generate SQL for YAML source/sink joints with columns/filter
    joint_names = {d.name for d in declarations_input}
    sql_gen = SQLGenerator()
    declarations = list(declarations_input)

    for i, decl in enumerate(declarations):
        if decl.joint_type in ("source", "sink") and decl.columns is not None and decl.sql is None:
            sql, sql_errors = sql_gen.generate(decl, joint_names)
            all_errors.extend(sql_errors)
            if sql and not sql_errors:
                declarations[i] = replace(decl, sql=sql)

    # Step 5: Infer upstream dependencies
    inferrer = UpstreamInferrer()
    for i, decl in enumerate(declarations):
        if decl.upstream is None and decl.joint_type != "source":
            upstream, up_errors = inferrer.infer(decl, joint_names)
            all_errors.extend(up_errors)
            declarations[i] = replace(decl, upstream=upstream)

    # Step 6: Convert declarations to core joints
    converter = JointConverter()
    joints = []
    for decl in declarations:
        joint, conv_errors = converter.convert(decl, engines)
        all_errors.extend(conv_errors)
        if joint is not None:
            joints.append(joint)

    # Step 7: Build assembly
    source_formats = {d.name: "yaml" if d.columns is not None else "sql" for d in declarations_input}
    result, build_errors = AssemblyBuilder().build(joints, catalogs, engines, profile, source_formats)
    all_errors.extend(build_errors)

    if all_errors:
        raise BridgeValidationError(all_errors)

    assert result is not None
    return result
