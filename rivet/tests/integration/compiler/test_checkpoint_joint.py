"""Integration tests: compiler handles checkpoint joints correctly.

Exercises the real compiler with checkpoint joints — no mocks for Rivet
internals. Verifies that checkpoint joints produce the expected CompiledJoint
fields, default write_strategy, validation errors, and warnings.
"""

from __future__ import annotations

from rivet_core.assembly import Assembly
from rivet_core.compiler import CompiledAssembly, compile
from rivet_core.models import Catalog, Joint
from rivet_core.plugins import PluginRegistry
from rivet_duckdb import DuckDBPlugin

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_registry() -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_builtins()
    DuckDBPlugin(reg)
    return reg


def _compile_pipeline(
    joints: list[Joint],
    *,
    catalogs: list[Catalog] | None = None,
    registry: PluginRegistry | None = None,
) -> CompiledAssembly:
    if registry is None:
        registry = _setup_registry()
    if catalogs is None:
        catalogs = [
            Catalog(name="c", type="filesystem", options={"path": "/tmp/fake", "format": "csv"})
        ]
    engines = [registry.get_engine_plugin("duckdb").create_engine("duckdb_primary", {})]
    for e in engines:
        if e.name not in {ce.name for ce in registry._compute_engines.values()}:
            registry.register_compute_engine(e)
    assembly = Assembly(joints)
    return compile(
        assembly,
        catalogs=catalogs,
        engines=engines,
        registry=registry,
        default_engine="duckdb_primary",
        introspect=False,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCheckpointCompiledFields:
    """Checkpoint joints produce correct CompiledJoint metadata."""

    def test_checkpoint_compiled_joint_fields(self):
        joints = [
            Joint(name="src", joint_type="source", catalog="c", table="orders"),
            Joint(
                name="cp",
                joint_type="checkpoint",
                catalog="c",
                table="t",
                write_strategy="replace",
                upstream=["src"],
            ),
            Joint(name="sink", joint_type="sink", catalog="c", table="output", upstream=["cp"]),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        cj_map = {cj.name: cj for cj in result.joints}
        cp = cj_map["cp"]
        assert cp.type == "checkpoint"
        assert cp.catalog == "c"
        assert cp.table == "t"
        assert cp.write_strategy == "replace"
        assert cp.sql is None

    def test_checkpoint_default_write_strategy(self):
        joints = [
            Joint(name="src", joint_type="source", catalog="c", table="orders"),
            Joint(name="cp", joint_type="checkpoint", catalog="c", table="t", upstream=["src"]),
            Joint(name="sink", joint_type="sink", catalog="c", table="output", upstream=["cp"]),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        cj_map = {cj.name: cj for cj in result.joints}
        assert cj_map["cp"].write_strategy == "replace"


class TestCheckpointValidationErrors:
    """Compiler catches missing required fields on checkpoint joints."""

    def test_checkpoint_no_catalog_produces_error(self):
        joints = [
            Joint(name="src", joint_type="source", catalog="c", table="orders"),
            Joint(name="cp", joint_type="checkpoint", table="t", upstream=["src"]),
            Joint(name="sink", joint_type="sink", catalog="c", table="output", upstream=["cp"]),
        ]
        result = _compile_pipeline(joints)

        catalog_errors = [
            e for e in result.errors if "catalog" in e.message.lower() and "cp" in e.message
        ]
        assert len(catalog_errors) >= 1

    def test_checkpoint_no_table_produces_error(self):
        joints = [
            Joint(name="src", joint_type="source", catalog="c", table="orders"),
            Joint(name="cp", joint_type="checkpoint", catalog="c", upstream=["src"]),
            Joint(name="sink", joint_type="sink", catalog="c", table="output", upstream=["cp"]),
        ]
        result = _compile_pipeline(joints)

        table_errors = [
            e for e in result.errors if "table" in e.message.lower() and "cp" in e.message
        ]
        assert len(table_errors) >= 1


class TestCheckpointWarnings:
    """Compiler warns when checkpoint has no downstream consumers."""

    def test_checkpoint_no_downstream_produces_warning(self):
        joints = [
            Joint(name="src", joint_type="source", catalog="c", table="orders"),
            Joint(name="cp", joint_type="checkpoint", catalog="c", table="t", upstream=["src"]),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        matching = [w for w in result.warnings if "cp" in w and "no downstream" in w.lower()]
        assert len(matching) >= 1
