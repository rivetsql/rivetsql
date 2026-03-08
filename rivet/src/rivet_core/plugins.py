"""Plugin contracts: ABC classes for all plugin types, ReferenceResolver, and PluginRegistry."""

from __future__ import annotations

import logging
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any

import pyarrow

from rivet_core.optimizer import AdapterPushdownResult, PushdownPlan

if TYPE_CHECKING:
    from rivet_core.introspection import CatalogNode, ObjectMetadata, ObjectSchema
    from rivet_core.metrics import PluginMetrics
    from rivet_core.models import Catalog, ComputeEngine, Joint, Material
    from rivet_core.strategies import MaterializedRef


@dataclass(frozen=True)
class UpstreamResolution:
    """Result of cross-joint adapter resolution at an engine boundary."""

    strategy: str  # "arrow_passthrough" | "native_reference" | "unsupported"
    table_reference: str | None = None
    materialized_table: pyarrow.Table | None = None
    message: str | None = None


@dataclass(frozen=True)
class CrossJointContext:
    """Context for CrossJointAdapter.resolve_upstream()."""

    producer_joint_name: str
    consumer_joint_name: str
    producer_catalog_type: str | None
    producer_table: str | None
    consumer_catalog_type: str | None


class CrossJointAdapter(ABC):
    """Declares how a consumer engine accesses upstream data at an engine boundary."""

    consumer_engine_type: str
    producer_engine_type: str

    @abstractmethod
    def resolve_upstream(
        self,
        producer_ref: MaterializedRef,
        consumer_engine: Any,
        joint_context: CrossJointContext,
    ) -> UpstreamResolution: ...


class ReferenceResolver(ABC):
    """Rewrites abstract table references into engine-native expressions at compile time."""

    @abstractmethod
    def resolve_references(
        self,
        sql: str,
        joint: Any,
        catalog: Any,
        compiled_joints: dict[str, Any] | None = None,
        catalog_map: dict[str, Any] | None = None,
        fused_group_joints: list[str] | None = None,
    ) -> str | None:
        """Return rewritten SQL, or None if unchanged.

        Parameters
        ----------
        fused_group_joints : list[str] | None
            Joint names in the same fused group. These are CTE aliases
            and must NOT be replaced with fully-qualified table names.
        """
        ...


class CatalogPlugin(ABC):
    """ABC for catalog plugins.

    Provides creation, validation, introspection, and table reference resolution.
    """

    type: str
    required_options: list[str]
    optional_options: dict[str, Any]
    credential_options: list[str]
    credential_groups: dict[str, list[str]] = {}
    env_var_hints: dict[str, str] = {}

    @abstractmethod
    def validate(self, options: dict[str, Any]) -> None: ...

    @abstractmethod
    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog: ...

    @abstractmethod
    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str: ...

    def resolve_table_reference(self, logical_name: str, catalog: Catalog) -> str:
        """Resolve logical table name to physical reference.

        Priority: table_map entry > default_table_reference > logical name passthrough.
        """
        table_map: dict[str, str] = catalog.options.get("table_map", {})
        if logical_name in table_map:
            return table_map[logical_name]
        return self.default_table_reference(logical_name, catalog.options)

    # Introspection (opt-in)
    def list_tables(self, catalog: Catalog) -> list[CatalogNode]:
        raise NotImplementedError

    def test_connection(self, catalog: Catalog) -> None:
        """Lightweight connectivity check. Override for faster checks.

        Default falls back to list_tables(). Plugins with expensive list_tables
        should override with a single cheap API call.
        Raises on failure.
        """
        self.list_tables(catalog)

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        raise NotImplementedError

    def get_metadata(self, catalog: Catalog, table: str) -> ObjectMetadata | None:
        return None

    def list_children(self, catalog: Catalog, path: list[str]) -> list[CatalogNode]:
        """List immediate children of a path.

        Default implementation calls list_tables() and filters to immediate children.
        Plugins with deep hierarchies (S3, filesystem) may override for efficiency.
        """
        all_nodes = self.list_tables(catalog)
        return [n for n in all_nodes if _is_immediate_child(n.path, path)]


def _is_immediate_child(node_path: list[str], parent_path: list[str]) -> bool:
    """Return True if node_path is exactly one segment deeper than parent_path with matching prefix."""
    return (
        len(node_path) == len(parent_path) + 1
        and node_path[: len(parent_path)] == parent_path
    )


class ComputeEnginePlugin(ABC):
    """ABC for compute engine plugins.

    Provides engine creation, validation, capability declaration, and SQL execution.
    """

    engine_type: str
    supported_catalog_types: dict[str, list[str]]  # catalog_type → capabilities

    @abstractmethod
    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine: ...

    @abstractmethod
    def validate(self, options: dict[str, Any]) -> None: ...

    @abstractmethod
    def execute_sql(
        self,
        engine: ComputeEngine,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        """Execute SQL on this engine. Every engine plugin MUST implement this."""
        ...

    @property
    def materialization_strategy_name(self) -> str | None:
        """Preferred materialization strategy name, or None for default (arrow)."""
        return None

    def get_reference_resolver(self) -> ReferenceResolver | None:
        return None

    def collect_metrics(self, execution_context: Any) -> PluginMetrics | None:
        return None


class ComputeEngineAdapter(ABC):
    """ABC for adapters contributing compute capabilities to an engine type for a catalog type."""

    target_engine_type: str
    catalog_type: str
    capabilities: list[str]
    source: str  # "engine_plugin" or "catalog_plugin"
    source_plugin: str | None = None  # plugin package name, e.g. "rivet_postgres"
    _registry: PluginRegistry | None = None  # set by PluginRegistry.register_adapter

    @abstractmethod
    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None) -> AdapterPushdownResult: ...

    @abstractmethod
    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any: ...


class SourcePlugin(ABC):
    """ABC for source plugins providing data reads from a catalog."""

    @abstractmethod
    def read(self, catalog: Catalog, joint: Joint, pushdown: Any | None) -> Material: ...


class SinkPlugin(ABC):
    """ABC for sink plugins providing data writes to a catalog."""

    @abstractmethod
    def write(self, catalog: Catalog, joint: Joint, material: Material, strategy: str) -> None: ...


logger = logging.getLogger(__name__)


class PluginRegistry:
    """Central registry for all plugin types.

    Discovers external plugins via Python entry points and registers built-in
    plugins first, then external plugins.
    """

    def __init__(self) -> None:
        self._catalog_plugins: dict[str, CatalogPlugin] = {}
        self._engine_plugins: dict[str, ComputeEnginePlugin] = {}
        self._compute_engines: dict[str, ComputeEngine] = {}
        self._compute_engine_types: dict[str, list[str]] = {}
        self._adapters: dict[tuple[str, str], ComputeEngineAdapter] = {}
        self._cross_joint_adapters: dict[tuple[str, str], CrossJointAdapter] = {}
        self._sources: dict[str, SourcePlugin] = {}
        self._sinks: dict[str, SinkPlugin] = {}
        self._discovered: bool = False
    @property
    def is_discovered(self) -> bool:
        """Return whether plugin discovery has already been performed."""
        return self._discovered

    # ── Registration ──────────────────────────────────────────────────

    def register_catalog_plugin(self, plugin: CatalogPlugin) -> None:
        t = plugin.type
        if t in self._catalog_plugins:
            existing = self._catalog_plugins[t]
            raise PluginRegistrationError(
                f"Catalog plugin type '{t}' is already registered by "
                f"{type(existing).__qualname__}. "
                f"Duplicate catalog_type conflict: check for duplicate plugin packages "
                f"and uninstall or exclude the conflicting one."
            )
        self._catalog_plugins[t] = plugin

    def register_engine_plugin(self, plugin: ComputeEnginePlugin) -> None:
        t = plugin.engine_type
        if t in self._engine_plugins:
            existing = self._engine_plugins[t]
            raise PluginRegistrationError(
                f"Engine plugin type '{t}' is already registered by "
                f"{type(existing).__qualname__}. "
                f"Duplicate engine_type conflict: check for duplicate plugin packages "
                f"and uninstall or exclude the conflicting one."
            )
        self._engine_plugins[t] = plugin

    def register_compute_engine(self, engine: ComputeEngine) -> None:
        if engine.name in self._compute_engines:
            raise PluginRegistrationError(
                f"Compute engine instance '{engine.name}' is already registered."
            )
        if engine.engine_type not in self._engine_plugins:
            raise PluginRegistrationError(
                f"No engine plugin registered for type '{engine.engine_type}'. "
                f"Register the engine plugin before creating instances."
            )
        self._compute_engines[engine.name] = engine
        self._compute_engine_types.setdefault(engine.engine_type, []).append(engine.name)

    def register_adapter(self, adapter: ComputeEngineAdapter) -> None:
        key = (adapter.target_engine_type, adapter.catalog_type)
        existing = self._adapters.get(key)
        if existing is not None:
            # catalog_plugin adapters take priority over engine_plugin adapters
            if adapter.source == existing.source:
                raise PluginRegistrationError(
                    f"Adapter conflict for ({key[0]}, {key[1]}): "
                    f"two '{adapter.source}' adapters registered for the same (engine_type, catalog_type) pair. "
                    f"Existing: {type(existing).__qualname__}, new: {type(adapter).__qualname__}. "
                    f"Check for duplicate plugin packages and uninstall or exclude the conflicting one."
                )
            elif adapter.source == "catalog_plugin":
                warnings.warn(
                    f"Adapter for ({key[0]}, {key[1]}): "
                    f"catalog_plugin adapter overriding engine_plugin adapter.",
                    stacklevel=2,
                )
            elif existing.source == "catalog_plugin":
                # Existing catalog_plugin adapter has priority; skip
                warnings.warn(
                    f"Adapter for ({key[0]}, {key[1]}): "
                    f"engine_plugin adapter ignored; catalog_plugin adapter already registered.",
                    stacklevel=2,
                )
                return
        self._adapters[key] = adapter
        adapter._registry = self

    def register_source(self, plugin: SourcePlugin) -> None:
        catalog_type = getattr(plugin, "catalog_type", None)
        if catalog_type is None:
            raise PluginRegistrationError(
                "SourcePlugin must have a 'catalog_type' attribute."
            )
        if catalog_type in self._sources:
            raise PluginRegistrationError(
                f"Source plugin for catalog type '{catalog_type}' is already registered."
            )
        self._sources[catalog_type] = plugin

    def register_sink(self, plugin: SinkPlugin) -> None:
        catalog_type = getattr(plugin, "catalog_type", None)
        if catalog_type is None:
            raise PluginRegistrationError(
                "SinkPlugin must have a 'catalog_type' attribute."
            )
        if catalog_type in self._sinks:
            raise PluginRegistrationError(
                f"Sink plugin for catalog type '{catalog_type}' is already registered."
            )
        self._sinks[catalog_type] = plugin

    def register_cross_joint_adapter(self, adapter: CrossJointAdapter) -> None:
        key = (adapter.consumer_engine_type, adapter.producer_engine_type)
        if key in self._cross_joint_adapters:
            existing = self._cross_joint_adapters[key]
            raise PluginRegistrationError(
                f"CrossJointAdapter for ({key[0]!r}, {key[1]!r}) is already registered by "
                f"{type(existing).__qualname__}. "
                f"Duplicate cross-joint adapter conflict: check for duplicate plugin packages "
                f"and uninstall or exclude the conflicting one."
            )
        self._cross_joint_adapters[key] = adapter

    # ── Lookup ────────────────────────────────────────────────────────

    def get_catalog_plugin(self, catalog_type: str) -> CatalogPlugin | None:
        return self._catalog_plugins.get(catalog_type)

    def get_engine_plugin(self, engine_type: str) -> ComputeEnginePlugin | None:
        return self._engine_plugins.get(engine_type)

    def get_compute_engine(self, instance_name: str) -> ComputeEngine | None:
        return self._compute_engines.get(instance_name)

    def get_adapter(
        self, engine_type: str, catalog_type: str
    ) -> ComputeEngineAdapter | None:
        return self._adapters.get((engine_type, catalog_type))

    def get_cross_joint_adapter(
        self, consumer_engine_type: str, producer_engine_type: str
    ) -> CrossJointAdapter | None:
        return self._cross_joint_adapters.get(
            (consumer_engine_type, producer_engine_type)
        )

    def resolve_capabilities(
        self, engine_type: str, catalog_type: str
    ) -> list[str] | None:
        adapter = self._adapters.get((engine_type, catalog_type))
        if adapter is not None:
            return adapter.capabilities
        plugin = self._engine_plugins.get(engine_type)
        if plugin is not None:
            caps = plugin.supported_catalog_types.get(catalog_type)
            if caps is not None:
                return caps
        return None

    # ── Discovery ─────────────────────────────────────────────────────

    def register_builtins(self) -> None:
        """Register built-in plugins (ArrowCatalog, FilesystemCatalog, etc.).

        Called before external plugin discovery. Imports are deferred to avoid
        circular imports and to keep builtins optional until they exist.
        """
        try:
            from rivet_core.builtins.arrow_catalog import (
                ArrowCatalogPlugin,
                ArrowComputeEnginePlugin,
                ArrowSink,
                ArrowSource,
            )

            self.register_catalog_plugin(ArrowCatalogPlugin())
            self.register_engine_plugin(ArrowComputeEnginePlugin())
            engine = ArrowComputeEnginePlugin().create_engine("arrow", {})
            self.register_compute_engine(engine)
            self.register_source(ArrowSource())
            self.register_sink(ArrowSink())
        except (ImportError, Exception):
            logger.debug("Arrow built-in plugins not available; skipping.")

        try:
            from rivet_core.builtins.filesystem_catalog import (
                FilesystemCatalogPlugin,
                FilesystemSource,
            )

            self.register_catalog_plugin(FilesystemCatalogPlugin())
            self.register_source(FilesystemSource())
        except (ImportError, Exception):
            logger.debug("Filesystem built-in plugins not available; skipping.")

    _ENTRY_POINT_GROUPS: dict[str, str] = {
        "rivet.catalogs": "register_catalog_plugin",
        "rivet.compute_engines": "register_engine_plugin",
        "rivet.compute_engine_adapters": "register_adapter",
        "rivet.sources": "register_source",
        "rivet.sinks": "register_sink",
        "rivet.cross_joint_adapters": "register_cross_joint_adapter",
    }

    def discover_plugins(self) -> None:
        """Discover and register external plugins via entry point groups.

        Supports two plugin registration patterns:

        1. **Monolithic** (``rivet.plugins`` group): Each entry point resolves
           to a callable ``plugin_fn(registry)`` that registers all components
           at once. This is the pattern all current plugins use.

        2. **Granular** (``rivet.catalogs``, ``rivet.compute_engines``, etc.):
           Each entry point resolves to a plugin class; the class is
           instantiated and registered via the corresponding ``register_*``
           method.

        Both patterns are loaded in alphabetical order by entry point name.
        """
        if self._discovered:
            return

        # ── Monolithic plugins (rivet.plugins) ───────────────────────
        eps = sorted(entry_points(group="rivet.plugins"), key=lambda ep: ep.name)
        for ep in eps:
            try:
                plugin_fn = ep.load()
                plugin_fn(self)
            except Exception as exc:
                raise PluginRegistrationError(
                    f"Failed to load plugin entry point '{ep.name}' "
                    f"from group 'rivet.plugins': {exc}"
                ) from exc

        # ── Granular plugins (rivet.catalogs, rivet.compute_engines, …)
        for group, register_method in self._ENTRY_POINT_GROUPS.items():
            eps = sorted(entry_points(group=group), key=lambda ep: ep.name)
            for ep in eps:
                try:
                    plugin_cls = ep.load()
                    plugin_instance = plugin_cls()
                    getattr(self, register_method)(plugin_instance)
                except Exception as exc:
                    raise PluginRegistrationError(
                        f"Failed to load plugin entry point '{ep.name}' "
                        f"from group '{group}': {exc}"
                    ) from exc

        self._discovered = True


class PluginRegistrationError(Exception):
    """Raised when plugin registration fails due to uniqueness or validation errors."""
