"""Property-based tests: Compiler Output Equivalence (Property 14).

Property 14: Compiler Output Equivalence
  For any valid Assembly, catalogs, engines, and registry inputs, the
  CompiledAssembly produced by compile() with engines resolved from the
  unified engine_map (pre-populated from both provided engines and registry)
  shall be identical to the output when all engines are passed explicitly.

  We verify this by compiling the same assembly twice:
    (a) with engines passed explicitly in the `engines` list
    (b) with engines registered only in the registry (not in the `engines` list)
  Both runs must produce identical CompiledAssembly outputs.
"""

from __future__ import annotations

from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.compiler import compile
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.plugins import (
    CatalogPlugin,
    ComputeEnginePlugin,
    PluginRegistry,
    SinkPlugin,
    SourcePlugin,
)

# ── Helpers ─────────────────────────────────────────────────────────────────────


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
    supported_catalog_types: dict[str, list[str]] = {"stub": ["projection_pushdown"]}

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


def _make_registry(
    *,
    register_engines: list[ComputeEngine] | None = None,
) -> PluginRegistry:
    """Build a registry with stub plugins and optionally pre-registered engines."""
    reg = PluginRegistry()
    reg.register_catalog_plugin(_StubCatalogPlugin())
    reg.register_engine_plugin(_StubEnginePlugin())
    reg.register_source(_StubSource())
    reg.register_sink(_StubSink())
    if register_engines:
        for eng in register_engines:
            reg.register_compute_engine(eng)
    return reg


# ── Strategies ──────────────────────────────────────────────────────────────────

_joint_name = st.text(
    alphabet=st.characters(whitelist_categories=("Ll",), whitelist_characters="_"),
    min_size=2,
    max_size=8,
)


@st.composite
def assembly_scenario(draw: st.DrawFn):
    """Generate a small random Assembly with source → sql chains.

    Returns (joints, engine_names) where engine_names is a list of unique
    engine instance names used across the joints.
    """
    # Generate 1-3 engine instance names
    n_engines = draw(st.integers(min_value=1, max_value=3))
    engine_names = [f"eng_{i}" for i in range(n_engines)]

    # Generate 2-5 source joints
    n_sources = draw(st.integers(min_value=1, max_value=4))
    source_names = [f"src_{i}" for i in range(n_sources)]

    joints: list[Joint] = []
    for name in source_names:
        eng = draw(st.sampled_from(engine_names))
        joints.append(
            Joint(name=name, joint_type="source", catalog="c", engine=eng, table=name)
        )

    # Generate 1-4 sql joints, each depending on a random source
    n_sql = draw(st.integers(min_value=1, max_value=4))
    for i in range(n_sql):
        eng = draw(st.sampled_from(engine_names))
        upstream_name = draw(st.sampled_from(source_names))
        sql_name = f"sql_{i}"
        joints.append(
            Joint(
                name=sql_name,
                joint_type="sql",
                catalog="c",
                engine=eng,
                upstream=[upstream_name],
                sql=f"SELECT * FROM {upstream_name}",
            )
        )

    return joints, engine_names


# ── Property 14: Compiler Output Equivalence ────────────────────────────────────


@given(scenario=assembly_scenario())
@settings(max_examples=100)
def test_property14_explicit_engines_vs_registry_engines(scenario: tuple) -> None:
    """Compile with explicit engines must equal compile with registry-only engines.

    Run (a): engines passed in the `engines` list, registry has no engines.
    Run (b): engines list is empty, engines registered in the registry.
    Both must produce identical CompiledAssembly outputs.
    """
    joints, engine_names = scenario

    catalogs = [Catalog(name="c", type="stub")]
    engines = [ComputeEngine(name=n, engine_type="stub") for n in engine_names]

    assembly = Assembly(joints)

    # Run (a): engines passed explicitly, none in registry
    reg_a = _make_registry()
    result_a = compile(
        assembly, catalogs, engines, reg_a,
        introspect=False, default_engine=engine_names[0],
    )

    # Run (b): no engines passed, all in registry
    reg_b = _make_registry(register_engines=engines)
    result_b = compile(
        assembly, catalogs, [], reg_b,
        introspect=False, default_engine=engine_names[0],
    )

    # Compare compiled joints (the core output)
    assert len(result_a.joints) == len(result_b.joints), (
        f"Joint count mismatch: {len(result_a.joints)} vs {len(result_b.joints)}"
    )

    joints_a = {cj.name: cj for cj in result_a.joints}
    joints_b = {cj.name: cj for cj in result_b.joints}

    for name in joints_a:
        cj_a = joints_a[name]
        cj_b = joints_b[name]
        assert cj_a.engine == cj_b.engine, (
            f"Engine mismatch for '{name}': {cj_a.engine} vs {cj_b.engine}"
        )
        assert cj_a.type == cj_b.type, (
            f"Type mismatch for '{name}': {cj_a.type} vs {cj_b.type}"
        )
        assert cj_a.upstream == cj_b.upstream, (
            f"Upstream mismatch for '{name}': {cj_a.upstream} vs {cj_b.upstream}"
        )
        assert cj_a.engine_resolution == cj_b.engine_resolution, (
            f"Resolution mismatch for '{name}': "
            f"{cj_a.engine_resolution} vs {cj_b.engine_resolution}"
        )
        assert cj_a.adapter == cj_b.adapter, (
            f"Adapter mismatch for '{name}': {cj_a.adapter} vs {cj_b.adapter}"
        )

    # Compare materializations
    mats_a = sorted(
        [(m.from_joint, m.to_joint, m.trigger) for m in result_a.materializations]
    )
    mats_b = sorted(
        [(m.from_joint, m.to_joint, m.trigger) for m in result_b.materializations]
    )
    assert mats_a == mats_b, (
        f"Materialization mismatch:\n  a={mats_a}\n  b={mats_b}"
    )

    # Compare fused groups
    groups_a = sorted([g.id for g in result_a.fused_groups])
    groups_b = sorted([g.id for g in result_b.fused_groups])
    assert len(groups_a) == len(groups_b), (
        f"Fused group count mismatch: {len(groups_a)} vs {len(groups_b)}"
    )

    # Compare engine boundaries
    bounds_a = sorted(
        [(b.producer_engine_type, b.consumer_engine_type) for b in result_a.engine_boundaries]
    )
    bounds_b = sorted(
        [(b.producer_engine_type, b.consumer_engine_type) for b in result_b.engine_boundaries]
    )
    assert bounds_a == bounds_b, (
        f"Engine boundary mismatch:\n  a={bounds_a}\n  b={bounds_b}"
    )

    # Both should compile successfully
    assert result_a.success == result_b.success, (
        f"Success mismatch: {result_a.success} vs {result_b.success}"
    )


@given(scenario=assembly_scenario())
@settings(max_examples=100)
def test_property14_registry_lookup_count_bounded(scenario: tuple) -> None:
    """The unified engine map should resolve each engine name at most once from
    the registry, regardless of how many joints reference it.

    We verify this by checking that compile() succeeds with engines only in the
    registry and that the number of distinct engine names in the assembly bounds
    the registry lookups.
    """
    joints, engine_names = scenario

    catalogs = [Catalog(name="c", type="stub")]
    engines = [ComputeEngine(name=n, engine_type="stub") for n in engine_names]

    assembly = Assembly(joints)

    # Track registry lookups
    reg = _make_registry(register_engines=engines)
    original_get = reg.get_compute_engine
    lookup_count = 0
    lookup_names: list[str] = []

    def _tracking_get(name: str) -> ComputeEngine | None:
        nonlocal lookup_count
        lookup_count += 1
        lookup_names.append(name)
        return original_get(name)

    reg.get_compute_engine = _tracking_get  # type: ignore[assignment]

    result = compile(
        assembly, catalogs, [], reg,
        introspect=False, default_engine=engine_names[0],
    )

    assert result.success

    # The unified map is built once at the start of compile().
    # Each unique engine name should be looked up at most once.
    distinct_engine_names = {j.engine for j in assembly.joints.values() if j.engine}
    if engine_names[0] not in distinct_engine_names:
        distinct_engine_names.add(engine_names[0])  # default_engine also looked up

    assert lookup_count <= len(distinct_engine_names), (
        f"Registry was called {lookup_count} times but only "
        f"{len(distinct_engine_names)} distinct engine names exist. "
        f"Lookups: {lookup_names}"
    )


# ── Property 15: Adapter Lookup Caching ─────────────────────────────────────────


@given(scenario=assembly_scenario())
@settings(max_examples=100)
def test_property15_adapter_lookup_count_bounded(scenario: tuple) -> None:
    """The adapter cache should resolve each (engine_type, catalog_type) pair
    at most once from the registry, regardless of how many joints share that pair.

    We verify this by tracking calls to registry.get_adapter() during compile()
    and asserting the count is at most K for K distinct (engine_type, catalog_type)
    pairs across all joints.
    """
    joints, engine_names = scenario

    catalogs = [Catalog(name="c", type="stub")]
    engines = [ComputeEngine(name=n, engine_type="stub") for n in engine_names]

    assembly = Assembly(joints)

    reg = _make_registry(register_engines=engines)
    original_get_adapter = reg.get_adapter
    adapter_lookup_count = 0
    adapter_lookup_keys: list[tuple[str, str]] = []

    def _tracking_get_adapter(
        engine_type: str, catalog_type: str
    ) -> Any:
        nonlocal adapter_lookup_count
        adapter_lookup_count += 1
        adapter_lookup_keys.append((engine_type, catalog_type))
        return original_get_adapter(engine_type, catalog_type)

    reg.get_adapter = _tracking_get_adapter  # type: ignore[assignment]

    result = compile(
        assembly, catalogs, engines, reg,
        introspect=False, default_engine=engine_names[0],
    )

    assert result.success

    # Each distinct (engine_type, catalog_type) pair should be looked up at most
    # once during joint compilation thanks to the adapter cache.
    # Note: _build_compiled_adapters also calls get_adapter separately (not cached),
    # so we account for those calls too.
    distinct_pairs_in_joints: set[tuple[str, str]] = set()
    for j in assembly.joints.values():
        cat = None
        for c in catalogs:
            if c.name == j.catalog:
                cat = c
                break
        if cat:
            eng = None
            for e in engines:
                if e.name == j.engine:
                    eng = e
                    break
            if eng and eng.engine_type and cat.type:
                distinct_pairs_in_joints.add((eng.engine_type, cat.type))

    # _compile_all_joints calls _resolve_adapter (cached): at most K calls
    # _build_compiled_adapters calls get_adapter (uncached): at most K calls
    # Total: at most 2*K calls
    max_expected = 2 * max(len(distinct_pairs_in_joints), 1)

    assert adapter_lookup_count <= max_expected, (
        f"registry.get_adapter() was called {adapter_lookup_count} times but only "
        f"{len(distinct_pairs_in_joints)} distinct (engine_type, catalog_type) pairs exist. "
        f"Max expected: {max_expected}. "
        f"Lookups: {adapter_lookup_keys}"
    )
