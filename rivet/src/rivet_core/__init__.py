"""rivet-core: foundational package for the Rivet data pipeline system."""

import rivet_core.testing as testing
from rivet_core.assembly import Assembly
from rivet_core.catalog_explorer import (
    CatalogExplorer,
    CatalogExplorerError,
    CatalogInfo,
    ConnectionResult,
    ExplorerNode,
    GeneratedSource,
    NodeDetail,
    SearchResult,
)
from rivet_core.compiler import compile
from rivet_core.credentials import CredentialResolver, Credentials
from rivet_core.executor import Executor
from rivet_core.models import Catalog, Column, ComputeEngine, Joint, Material, Schema
from rivet_core.optimizer import AdapterPushdownResult
from rivet_core.plugins import (
    CatalogPlugin,
    ComputeEngineAdapter,
    ComputeEnginePlugin,
    CrossJointAdapter,
    CrossJointContext,
    PluginRegistry,
    ReferenceResolver,
    SinkPlugin,
    SourcePlugin,
    UpstreamResolution,
)

__all__ = [
    "AdapterPushdownResult",
    "Assembly",
    "Catalog",
    "CatalogExplorer",
    "CatalogExplorerError",
    "CatalogInfo",
    "Column",
    "ComputeEngine",
    "compile",
    "ConnectionResult",
    "CredentialResolver",
    "Credentials",
    "Executor",
    "ExplorerNode",
    "GeneratedSource",
    "Joint",
    "Material",
    "NodeDetail",
    "Schema",
    "SearchResult",
    "CatalogPlugin",
    "ComputeEngineAdapter",
    "ComputeEnginePlugin",
    "CrossJointAdapter",
    "CrossJointContext",
    "PluginRegistry",
    "ReferenceResolver",
    "SinkPlugin",
    "SourcePlugin",
    "UpstreamResolution",
    "testing",
]
