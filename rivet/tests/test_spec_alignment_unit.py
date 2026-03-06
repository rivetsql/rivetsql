"""Unit tests for structural requirements from spec-alignment-fixes.

These tests verify structural properties of the codebase that must hold
after the spec alignment changes are applied.
"""

from __future__ import annotations

import dataclasses
import inspect
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ── Req 1.3: ComputeEngine is not frozen ─────────────────────────────


def test_compute_engine_is_not_frozen():
    """ComputeEngine dataclass SHALL remain non-frozen (mutable)."""
    from rivet_core.models import ComputeEngine

    engine = ComputeEngine(name="test", engine_type="arrow")
    engine.name = "renamed"
    assert engine.name == "renamed"


# ── Req 3.1: _ARROW_TABLES module-level global no longer exists ──────


def test_arrow_tables_module_global_removed():
    """ArrowCatalogPlugin SHALL NOT use module-level mutable state for table storage."""
    import rivet_core.builtins.arrow_catalog as mod

    assert not hasattr(mod, "_ARROW_TABLES"), (
        "_ARROW_TABLES should not exist as a module-level attribute"
    )


# ── Req 3.4: ArrowSource/ArrowSink accept table_store constructor ────


def test_arrow_source_accepts_table_store():
    """ArrowSource SHALL access table storage through constructor injection."""
    from rivet_core.builtins.arrow_catalog import ArrowSource

    sig = inspect.signature(ArrowSource.__init__)
    params = list(sig.parameters.keys())
    assert "table_store" in params, (
        f"ArrowSource.__init__ should accept 'table_store' param, got: {params}"
    )


def test_arrow_sink_accepts_table_store():
    """ArrowSink SHALL access table storage through constructor injection."""
    from rivet_core.builtins.arrow_catalog import ArrowSink

    sig = inspect.signature(ArrowSink.__init__)
    params = list(sig.parameters.keys())
    assert "table_store" in params, (
        f"ArrowSink.__init__ should accept 'table_store' param, got: {params}"
    )


# ── Req 4.2: discover_plugins does not query rivet.plugins group ─────


def test_discover_plugins_does_not_use_rivet_plugins_group():
    """PluginRegistry queries both monolithic 'rivet.plugins' and granular groups."""
    from rivet_core.plugins import PluginRegistry

    registry = PluginRegistry()

    with patch("rivet_core.plugins.entry_points") as mock_ep:
        mock_ep.return_value = []
        registry.discover_plugins()

        # Collect all group= arguments from calls
        groups_queried = [
            call.kwargs.get("group") or (call.args[0] if call.args else None)
            for call in mock_ep.call_args_list
        ]
        # All five granular groups must be queried
        for g in ["rivet.catalogs", "rivet.compute_engines",
                   "rivet.compute_engine_adapters", "rivet.sources", "rivet.sinks"]:
            assert g in groups_queried, f"Expected group {g!r} to be queried"


# ── Req 4.8: PluginRegistrationError includes group name ─────────────


def test_plugin_registration_error_includes_group_name():
    """PluginRegistrationError SHALL include the entry point group name."""
    from rivet_core.plugins import PluginRegistrationError, PluginRegistry

    registry = PluginRegistry()

    bad_ep = MagicMock()
    bad_ep.name = "bad_plugin"
    bad_ep.load.side_effect = RuntimeError("load failed")

    with patch("rivet_core.plugins.entry_points") as mock_ep:
        def side_effect(group):
            if group == "rivet.catalogs":
                return [bad_ep]
            return []
        mock_ep.side_effect = side_effect

        with pytest.raises(PluginRegistrationError, match="rivet.catalogs"):
            registry.discover_plugins()


# ── Req 6.1, 7.1, 8.1: JointDeclaration new fields default to None ──


def test_joint_declaration_has_dialect_field():
    """JointDeclaration SHALL include a 'dialect' field defaulting to None."""
    from rivet_config.models import JointDeclaration

    decl = JointDeclaration(name="t", joint_type="source", source_path=Path("x.yaml"))
    assert decl.dialect is None


def test_joint_declaration_has_path_field():
    """JointDeclaration SHALL include a 'path' field defaulting to None."""
    from rivet_config.models import JointDeclaration

    decl = JointDeclaration(name="t", joint_type="source", source_path=Path("x.yaml"))
    assert decl.path is None


def test_joint_declaration_has_source_format_field():
    """JointDeclaration SHALL include a 'source_format' field defaulting to None."""
    from rivet_config.models import JointDeclaration

    decl = JointDeclaration(name="t", joint_type="source", source_path=Path("x.yaml"))
    assert decl.source_format is None


# ── Req 9.1: JointDeclaration uses source_path, not source_file ──────


def test_joint_declaration_has_source_path():
    """JointDeclaration SHALL use 'source_path' field."""
    from rivet_config.models import JointDeclaration

    decl = JointDeclaration(name="t", joint_type="source", source_path=Path("x.yaml"))
    assert decl.source_path == Path("x.yaml")


def test_joint_declaration_has_no_source_file():
    """JointDeclaration SHALL NOT have a 'source_file' field."""
    from rivet_config.models import JointDeclaration

    fields = {f.name for f in dataclasses.fields(JointDeclaration)}
    assert "source_file" not in fields, (
        "JointDeclaration should use 'source_path', not 'source_file'"
    )


# ── Req 12.1: build_assembly accepts ProjectDeclaration ──────────────


def test_build_assembly_accepts_project_declaration():
    """build_assembly SHALL accept a ProjectDeclaration parameter."""
    from rivet_bridge.forward import build_assembly

    sig = inspect.signature(build_assembly)
    param_names = list(sig.parameters.keys())
    assert "project" in param_names, (
        f"build_assembly should accept 'project' param, got: {param_names}"
    )
