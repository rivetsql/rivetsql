"""Property tests for compiler checkpoint source resolution.

Property 8: Compiler pre-resolves checkpoint_sources on FusedGroups.

For any pipeline with checkpoint joints consumed by downstream groups,
the compiler populates checkpoint_sources on each downstream FusedGroup
with the correct adapter resolution for (downstream_engine_type, checkpoint_catalog_type).

Validates: Requirements 6.1, 6.2, 6.3
"""

from __future__ import annotations

from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.compiler import compile
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.optimizer import EMPTY_RESIDUAL, AdapterPushdownResult
from rivet_core.plugins import (
    CatalogPlugin,
    ComputeEngineAdapter,
    ComputeEnginePlugin,
    PluginRegistry,
    SinkPlugin,
    SourcePlugin,
)

# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class _StubCatalogPlugin(CatalogPlugin):
    type = "stub"
    required_options: list[str] = []
    optional_options: dict[str, Any] = {}
    credential_options: list[str] = []

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        return Catalog(name=name, type=self.type, options=options)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name


class _StubEnginePlugin(ComputeEnginePlugin):
    engine_type = "stub"
    supported_catalog_types: dict[str, list[str]] = {
        "stub": ["projection_pushdown", "predicate_pushdown", "limit_pushdown"],
    }

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine: Any, sql: Any, input_tables: Any) -> Any:
        raise NotImplementedError


class _StubSource(SourcePlugin):
    catalog_type = "stub"

    def read(self, catalog: Any, joint: Any, pushdown: Any = None) -> Any:
        return None


class _StubSink(SinkPlugin):
    catalog_type = "stub"

    def write(self, catalog: Any, joint: Any, material: Any, strategy: str) -> None:
        pass


class _StubAdapter(ComputeEngineAdapter):
    target_engine_type = "stub"
    catalog_type = "stub"
    capabilities = ["projection_pushdown"]
    source = "engine_plugin"

    def read_dispatch(
        self, engine: Any, catalog: Any, joint: Any, pushdown: Any = None
    ) -> AdapterPushdownResult:
        return AdapterPushdownResult(material=None, residual=EMPTY_RESIDUAL)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        return None


def _make_engine_plugin(engine_type: str, catalog_types: list[str]) -> ComputeEnginePlugin:
    caps = {ct: ["projection_pushdown"] for ct in catalog_types}
    return type(
        f"Engine_{engine_type}",
        (_StubEnginePlugin,),
        {"engine_type": engine_type, "supported_catalog_types": caps},
    )()


def _make_catalog_plugin(catalog_type: str) -> CatalogPlugin:
    return type(
        f"Cat_{catalog_type}",
        (_StubCatalogPlugin,),
        {"type": catalog_type},
    )()


def _make_adapter(engine_type: str, catalog_type: str) -> ComputeEngineAdapter:
    return type(
        f"Adapter_{engine_type}_{catalog_type}",
        (_StubAdapter,),
        {"target_engine_type": engine_type, "catalog_type": catalog_type},
    )()


def _make_source(catalog_type: str) -> SourcePlugin:
    return type(f"Src_{catalog_type}", (_StubSource,), {"catalog_type": catalog_type})()


def _make_sink(catalog_type: str) -> SinkPlugin:
    return type(f"Sink_{catalog_type}", (_StubSink,), {"catalog_type": catalog_type})()


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_ENGINE_TYPES = st.sampled_from(["duckdb_t", "spark_t", "polars_t"])
_CATALOG_TYPES = st.sampled_from(["filesystem", "glue", "unity"])


@st.composite
def _checkpoint_pipeline_config(draw: st.DrawFn) -> dict[str, Any]:
    """Generate a pipeline: source -> checkpoint -> transform -> sink.

    The checkpoint writes to a catalog of a random type, and the downstream
    transform runs on a potentially different engine type.
    """
    cp_catalog_type = draw(_CATALOG_TYPES)
    upstream_engine_type = draw(_ENGINE_TYPES)
    downstream_engine_type = draw(_ENGINE_TYPES)
    has_adapter = draw(st.booleans())

    return {
        "cp_catalog_type": cp_catalog_type,
        "upstream_engine_type": upstream_engine_type,
        "downstream_engine_type": downstream_engine_type,
        "has_adapter": has_adapter,
    }


# ---------------------------------------------------------------------------
# Property 8: Compiler pre-resolves checkpoint_sources on FusedGroups
# ---------------------------------------------------------------------------


# Feature: checkpoint-source-resolution, Property 8
@given(config=_checkpoint_pipeline_config())
@settings(max_examples=100)
def test_compiler_resolves_checkpoint_sources(config: dict[str, Any]) -> None:
    """For any checkpoint pipeline, the compiler populates checkpoint_sources
    on downstream FusedGroups with correct adapter resolution."""
    cp_cat_type = config["cp_catalog_type"]
    up_et = config["upstream_engine_type"]
    down_et = config["downstream_engine_type"]
    has_adapter = config["has_adapter"]

    # Build registry
    reg = PluginRegistry()
    reg.register_catalog_plugin(_make_catalog_plugin(cp_cat_type))
    reg.register_engine_plugin(_make_engine_plugin(up_et, [cp_cat_type]))
    if down_et != up_et:
        reg.register_engine_plugin(_make_engine_plugin(down_et, [cp_cat_type]))

    up_eng = ComputeEngine(name="up_eng", engine_type=up_et)
    down_eng = ComputeEngine(name="down_eng", engine_type=down_et)
    reg.register_compute_engine(up_eng)
    if down_et != up_et:
        reg.register_compute_engine(down_eng)

    # Register adapter for upstream engine (checkpoint's own engine)
    reg.register_adapter(_make_adapter(up_et, cp_cat_type))
    reg.register_source(_make_source(cp_cat_type))
    reg.register_sink(_make_sink(cp_cat_type))

    # Optionally register adapter for downstream engine
    if has_adapter and down_et != up_et:
        reg.register_adapter(_make_adapter(down_et, cp_cat_type))

    # Build pipeline: source -> checkpoint -> transform -> sink
    joints = [
        Joint(name="src", joint_type="source", catalog="cp_cat", engine="up_eng"),
        Joint(
            name="cp",
            joint_type="checkpoint",
            catalog="cp_cat",
            table="cp_table",
            upstream=["src"],
            sql="SELECT * FROM src",
            engine="up_eng",
        ),
        Joint(
            name="transform",
            joint_type="sql",
            sql="SELECT * FROM cp",
            upstream=["cp"],
            engine="down_eng" if down_et != up_et else "up_eng",
        ),
        Joint(
            name="out",
            joint_type="sink",
            catalog="cp_cat",
            table="out_table",
            upstream=["transform"],
            engine="down_eng" if down_et != up_et else "up_eng",
        ),
    ]

    assembly = Assembly(joints)
    catalogs = [Catalog(name="cp_cat", type=cp_cat_type)]
    engines = [up_eng]
    if down_et != up_et:
        engines.append(down_eng)

    result = compile(
        assembly,
        catalogs,
        engines,
        reg,
        introspect=False,
        default_engine="up_eng",
    )

    assert result.success, f"Compilation failed: {result.errors}"

    # Find the downstream group containing "transform"
    downstream_group = None
    for g in result.fused_groups:
        if "transform" in g.joints:
            downstream_group = g
            break
    assert downstream_group is not None, "No group found containing 'transform'"

    # The checkpoint "cp" is in a different group from "transform" because
    # checkpoint joints create materialization boundaries.
    cp_group = None
    for g in result.fused_groups:
        if "cp" in g.joints:
            cp_group = g
            break
    assert cp_group is not None

    # If checkpoint and transform are in different groups, checkpoint_sources
    # should be populated on the downstream group.
    if cp_group.id != downstream_group.id:
        assert "cp" in downstream_group.checkpoint_sources, (
            f"checkpoint_sources missing 'cp' on downstream group. "
            f"Groups: {[g.id for g in result.fused_groups]}"
        )
        cp_info = downstream_group.checkpoint_sources["cp"]
        assert cp_info.checkpoint_joint == "cp"
        assert cp_info.catalog == "cp_cat"
        assert cp_info.catalog_type == cp_cat_type
        assert cp_info.table == "cp_table"

        # Adapter resolution: should match downstream engine type
        if has_adapter or down_et == up_et:
            # Adapter registered for downstream engine (or same engine)
            assert cp_info.adapter == f"{down_et}:{cp_cat_type}"
        else:
            # No adapter for downstream engine
            assert cp_info.adapter is None


# Feature: checkpoint-source-resolution, Property 8 (adapter in compiled output)
@given(config=_checkpoint_pipeline_config())
@settings(max_examples=50)
def test_checkpoint_adapter_in_compiled_adapters(config: dict[str, Any]) -> None:
    """When a checkpoint-downstream adapter is resolved, it appears in compiled_adapters."""
    cp_cat_type = config["cp_catalog_type"]
    up_et = config["upstream_engine_type"]
    down_et = config["downstream_engine_type"]
    has_adapter = config["has_adapter"]

    # Skip same-engine cases — adapter already present from checkpoint's own compilation
    if down_et == up_et:
        return

    reg = PluginRegistry()
    reg.register_catalog_plugin(_make_catalog_plugin(cp_cat_type))
    reg.register_engine_plugin(_make_engine_plugin(up_et, [cp_cat_type]))
    reg.register_engine_plugin(_make_engine_plugin(down_et, [cp_cat_type]))

    up_eng = ComputeEngine(name="up_eng", engine_type=up_et)
    down_eng = ComputeEngine(name="down_eng", engine_type=down_et)
    reg.register_compute_engine(up_eng)
    reg.register_compute_engine(down_eng)

    reg.register_adapter(_make_adapter(up_et, cp_cat_type))
    reg.register_source(_make_source(cp_cat_type))
    reg.register_sink(_make_sink(cp_cat_type))

    if has_adapter:
        reg.register_adapter(_make_adapter(down_et, cp_cat_type))

    joints = [
        Joint(name="src", joint_type="source", catalog="cp_cat", engine="up_eng"),
        Joint(
            name="cp",
            joint_type="checkpoint",
            catalog="cp_cat",
            table="cp_table",
            upstream=["src"],
            sql="SELECT * FROM src",
            engine="up_eng",
        ),
        Joint(
            name="transform",
            joint_type="sql",
            sql="SELECT * FROM cp",
            upstream=["cp"],
            engine="down_eng",
        ),
        Joint(
            name="out",
            joint_type="sink",
            catalog="cp_cat",
            table="out_table",
            upstream=["transform"],
            engine="down_eng",
        ),
    ]

    assembly = Assembly(joints)
    catalogs = [Catalog(name="cp_cat", type=cp_cat_type)]
    engines = [up_eng, down_eng]

    result = compile(
        assembly,
        catalogs,
        engines,
        reg,
        introspect=False,
        default_engine="up_eng",
    )

    assert result.success, f"Compilation failed: {result.errors}"

    adapter_keys = {(a.engine_type, a.catalog_type) for a in result.adapters}

    if has_adapter:
        assert (down_et, cp_cat_type) in adapter_keys, (
            f"Expected adapter ({down_et}, {cp_cat_type}) in compiled_adapters. Got: {adapter_keys}"
        )


# Feature: checkpoint-source-resolution, Property 8 (warning on missing adapter)
@given(config=_checkpoint_pipeline_config())
@settings(max_examples=50)
def test_checkpoint_missing_adapter_emits_warning(config: dict[str, Any]) -> None:
    """When no adapter exists for (downstream_engine, checkpoint_catalog), a warning is emitted."""
    cp_cat_type = config["cp_catalog_type"]
    up_et = config["upstream_engine_type"]
    down_et = config["downstream_engine_type"]

    # Force no adapter for downstream engine
    if down_et == up_et:
        return  # Same engine always has adapter

    reg = PluginRegistry()
    reg.register_catalog_plugin(_make_catalog_plugin(cp_cat_type))
    reg.register_engine_plugin(_make_engine_plugin(up_et, [cp_cat_type]))
    reg.register_engine_plugin(_make_engine_plugin(down_et, [cp_cat_type]))

    up_eng = ComputeEngine(name="up_eng", engine_type=up_et)
    down_eng = ComputeEngine(name="down_eng", engine_type=down_et)
    reg.register_compute_engine(up_eng)
    reg.register_compute_engine(down_eng)

    reg.register_adapter(_make_adapter(up_et, cp_cat_type))
    # Deliberately NOT registering adapter for down_et
    reg.register_source(_make_source(cp_cat_type))
    reg.register_sink(_make_sink(cp_cat_type))

    joints = [
        Joint(name="src", joint_type="source", catalog="cp_cat", engine="up_eng"),
        Joint(
            name="cp",
            joint_type="checkpoint",
            catalog="cp_cat",
            table="cp_table",
            upstream=["src"],
            sql="SELECT * FROM src",
            engine="up_eng",
        ),
        Joint(
            name="transform",
            joint_type="sql",
            sql="SELECT * FROM cp",
            upstream=["cp"],
            engine="down_eng",
        ),
        Joint(
            name="out",
            joint_type="sink",
            catalog="cp_cat",
            table="out_table",
            upstream=["transform"],
            engine="down_eng",
        ),
    ]

    assembly = Assembly(joints)
    catalogs = [Catalog(name="cp_cat", type=cp_cat_type)]
    engines = [up_eng, down_eng]

    result = compile(
        assembly,
        catalogs,
        engines,
        reg,
        introspect=False,
        default_engine="up_eng",
    )

    assert result.success, f"Compilation failed: {result.errors}"

    # Should have a warning about missing adapter for checkpoint resolution
    adapter_warnings = [
        w for w in result.warnings if "No adapter for" in w and "checkpoint" in w.lower()
    ]
    assert len(adapter_warnings) > 0, (
        f"Expected warning about missing adapter for ({down_et}, {cp_cat_type}). "
        f"Warnings: {result.warnings}"
    )
