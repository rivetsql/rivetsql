"""Catalog commands: shared startup helper and command handlers."""

from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

from rivet_bridge import register_optional_plugins
from rivet_bridge.catalogs import CatalogInstantiator
from rivet_bridge.engines import EngineInstantiator
from rivet_cli.errors import format_upstream_error
from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS, USAGE_ERROR
from rivet_config import load_config
from rivet_core import CatalogExplorer, NodeDetail, PluginRegistry

if TYPE_CHECKING:
    from rivet_cli.app import GlobalOptions

_CONNECT_TIMEOUT_S = 30.0


def _startup(globals_: GlobalOptions) -> CatalogExplorer | int:
    """Shared startup for all catalog commands.

    Parses rivet.yaml, resolves profile, instantiates catalogs/engines,
    probes connections in parallel with per-catalog timeouts, and constructs
    a CatalogExplorer.

    Returns a CatalogExplorer on success, or an exit code (int) on failure.
    """
    config_result = load_config(globals_.project_path, globals_.profile)
    if not config_result.success:
        for e in config_result.errors:
            print(format_upstream_error(e.code if hasattr(e, "code") else "CFG", e.message, getattr(e, "remediation", None), globals_.color), file=sys.stderr)
        return GENERAL_ERROR

    profile = config_result.profile
    if profile is None:
        print(format_upstream_error("RVT-853", "No profile resolved.", "Check profiles.yaml and --profile flag.", globals_.color), file=sys.stderr)
        return GENERAL_ERROR

    registry = PluginRegistry()
    registry.register_builtins()
    register_optional_plugins(registry)

    catalogs, cat_errors = CatalogInstantiator().instantiate_all(profile, registry)
    for e in cat_errors:  # type: ignore[assignment]
        print(format_upstream_error(e.code, e.message, e.remediation or "", globals_.color), file=sys.stderr)  # type: ignore[attr-defined]

    engines, eng_errors = EngineInstantiator().instantiate_all(profile, registry)
    for e in eng_errors:  # type: ignore[assignment]
        print(format_upstream_error(e.code, e.message, e.remediation or "", globals_.color), file=sys.stderr)  # type: ignore[attr-defined]

    if not catalogs:
        print(format_upstream_error("RVT-870", "All catalogs failed to instantiate.", "Check catalog configuration in profiles.yaml.", globals_.color), file=sys.stderr)
        return GENERAL_ERROR

    # Probe connections in parallel with per-catalog timeouts
    connection_status: dict[str, tuple[bool, str | None]] = {}

    def _probe(name: str) -> tuple[str, bool, str | None]:
        cat = catalogs[name]
        plugin = registry.get_catalog_plugin(cat.type)
        if plugin is None:
            return name, False, f"No plugin for type '{cat.type}'"
        try:
            check = getattr(plugin, "test_connection", None)
            if check is not None:
                check(cat)
            else:
                plugin.list_tables(cat)
            return name, True, None
        except Exception as exc:
            return name, False, str(exc)

    with ThreadPoolExecutor(max_workers=len(catalogs)) as pool:
        futures = {pool.submit(_probe, n): n for n in catalogs}
        for future in as_completed(futures, timeout=_CONNECT_TIMEOUT_S):
            name = futures[future]
            try:
                _, connected, error = future.result(timeout=_CONNECT_TIMEOUT_S)
            except Exception as exc:
                connected, error = False, str(exc)
            connection_status[name] = (connected, error)

    # Report failures inline
    for name, (connected, error) in connection_status.items():
        if not connected:
            print(format_upstream_error("RVT-870", f"Catalog '{name}' connection failed: {error}", "Check catalog configuration and network connectivity.", globals_.color), file=sys.stderr)

    # Exit 1 if ALL catalogs failed
    if not any(c for c, _ in connection_status.values()):
        return GENERAL_ERROR

    # Construct CatalogExplorer with pre-probed connection status (skip redundant probe)
    explorer = CatalogExplorer(catalogs, engines, registry, skip_probe=True)
    explorer._connection_status = connection_status

    return explorer


def catalog_list(
    explorer: CatalogExplorer,
    globals: GlobalOptions,
    catalog_name: str | None = None,
    depth: int = 0,
    format: str = "text",
) -> int:
    """Handle `rivet catalog list` command.

    - No arguments: list all catalogs with connection status and type summary.
    - catalog_name: list only that catalog's contents.
    - depth: expand tree to depth N (0=catalogs, 1=schemas, 2=tables, 3=columns).
    - format: text | tree | json — delegate to appropriate renderer.

    Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6
    """
    from rivet_cli.rendering.catalog_json import render_list_json
    from rivet_cli.rendering.catalog_text import render_catalog_list, render_catalog_tree

    globals_ = globals
    all_catalogs = explorer.list_catalogs()

    # Filter to a single catalog if name argument provided (Req 9.3)
    if catalog_name is not None:
        matching = [c for c in all_catalogs if c.name == catalog_name]
        if not matching:
            from rivet_cli.errors import CLIError, format_cli_error
            err = CLIError(
                code="RVT-871",
                message=f"Catalog '{catalog_name}' not found.",
                remediation="Run 'rivet catalog list' to see available catalogs.",
            )
            print(format_cli_error(err, globals_.color), file=sys.stderr)
            return USAGE_ERROR
        all_catalogs = matching

    if format == "json":
        # Build children dict for depth > 0 (Req 9.4)
        children: dict[str, list] = {}  # type: ignore[type-arg]
        if depth > 0:
            for cat in all_catalogs:
                if cat.connected:
                    children[cat.name] = explorer.list_children([cat.name])
        print(render_list_json(all_catalogs, children if depth > 0 else None))
        return SUCCESS

    if format == "tree":
        # Tree view with indentation (Req 9.5)
        output = render_catalog_tree(
            all_catalogs,
            explorer.list_children,
            depth,
            globals_.color,
        )
        print(output)
        return SUCCESS

    # Default: text table (Req 9.1, 9.6)
    if depth == 0:
        print(render_catalog_list(all_catalogs, globals_.color))
        return SUCCESS

    # depth > 0 with text format: use tree renderer
    output = render_catalog_tree(
        all_catalogs,
        explorer.list_children,
        depth,
        globals_.color,
    )
    print(output)
    return SUCCESS


def catalog_describe(explorer: CatalogExplorer, path: str, stats: bool, format: str, globals: GlobalOptions) -> int:
    """Handle `rivet catalog describe <catalog>.<schema>.<table>` command.

    Displays full column schema (name, type, nullable, default, constraints)
    and metadata. With --stats, also displays column-level statistics.
    With --format json, outputs as JSON object.

    Exit code 10 with RVT-871 if table not found.

    Requirements: 10.1, 10.2, 10.3, 10.4
    """
    from rivet_cli.errors import CLIError, format_cli_error

    # Parse path: must be at least catalog.table (2 segments)
    parts = path.split(".")
    if len(parts) < 2:
        err = CLIError(
            code="RVT-871",
            message=f"Invalid path '{path}'. Expected format: catalog.schema.table or catalog.table.",
            remediation="Provide a dot-separated path with at least catalog and table.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return USAGE_ERROR

    # Verify catalog exists
    all_catalogs = explorer.list_catalogs()
    catalog_names = {c.name for c in all_catalogs}
    if parts[0] not in catalog_names:
        err = CLIError(
            code="RVT-871",
            message=f"Catalog '{parts[0]}' not found.",
            remediation=f"Available catalogs: {', '.join(sorted(catalog_names)) or 'none'}.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return USAGE_ERROR

    try:
        detail = explorer.get_node_detail(parts)
    except Exception as exc:
        err = CLIError(
            code="RVT-871",
            message=f"Table '{path}' not found: {exc}",
            remediation="Check the path format and that the table exists in the catalog.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return USAGE_ERROR

    # If schema is None, the table was not found
    if detail.schema is None and len(parts) >= 2:
        err = CLIError(
            code="RVT-871",
            message=f"Table '{path}' not found or schema unavailable.",
            remediation="Check the path format and that the table exists in the catalog.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return USAGE_ERROR

    # Optionally fetch stats (column-level statistics)
    if stats:
        stats_metadata = explorer.get_table_stats(parts)
        if stats_metadata is not None:
            detail = NodeDetail(
                node=detail.node,
                schema=detail.schema,
                metadata=stats_metadata,
                children_count=detail.children_count,
            )

    if format == "json":
        from rivet_cli.rendering.catalog_json import render_describe_json
        print(render_describe_json(detail))
    else:
        from rivet_cli.rendering.catalog_text import render_node_detail
        print(render_node_detail(detail, globals.color, show_stats=stats))

    return SUCCESS


def catalog_search(explorer: CatalogExplorer, query: str, limit: int, format: str, globals: GlobalOptions) -> int:
    """Handle `rivet catalog search` command.

    Performs fuzzy search across all connected catalogs and displays ranked
    results with qualified names and node types.

    - limit: cap results (default 20)
    - format: text | json

    Requirements: 11.1, 11.2, 11.3
    """
    from rivet_cli.rendering.catalog_json import render_search_json
    from rivet_cli.rendering.catalog_text import render_search_results

    results = explorer.search(query, limit=limit)

    if format == "json":
        print(render_search_json(results))
    else:
        print(render_search_results(results, globals.color))

    return SUCCESS


def catalog_generate(
    explorer: CatalogExplorer,
    path: str,
    format: str,
    output: str | None,
    stdout: bool,
    name: str | None,
    columns: list[str] | None,
    globals: GlobalOptions,
) -> int:
    """Handle `rivet catalog generate` command.

    Generate a source joint declaration from <catalog>.<schema>.<table> path.
    - Default: write YAML to sources/<name>.yaml
    - --format sql: generate SQL instead of YAML
    - --output <path>: write to specified path
    - --stdout: print to stdout instead of writing file
    - --name <name>: override auto-generated name
    - --columns col1,col2: generate with only specified columns
    - Exit code 10 with RVT-874 on failure, RVT-877 on file write failure

    Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7
    """
    import os

    from rivet_cli.errors import CLIError, format_cli_error, format_upstream_error
    from rivet_cli.rendering.catalog_text import render_generate_confirmation
    from rivet_core.catalog_explorer import CatalogExplorerError, sanitize_name

    # Parse path into segments (Req 12.1)
    parts = path.split(".")
    if len(parts) < 2:
        err = CLIError(
            code="RVT-874",
            message=f"Invalid table path '{path}'. Expected format: catalog.schema.table or catalog.table.",
            remediation="Provide a fully qualified table path, e.g. 'mydb.public.users'.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return USAGE_ERROR

    node_path = parts

    # Generate source via CatalogExplorer
    try:
        source = explorer.generate_source(node_path, format=format, columns=columns)
    except CatalogExplorerError as exc:
        print(format_upstream_error(exc.error.code, exc.error.message, getattr(exc.error, "remediation", None), globals.color), file=sys.stderr)
        return USAGE_ERROR

    # Override name if provided (Req 12.5)
    content = source.content
    suggested_filename = source.suggested_filename
    if name is not None:
        # Replace the auto-generated name in the content
        auto_name = sanitize_name(parts[-1])
        if format == "yaml":
            content = content.replace(f"name: {auto_name}", f"name: {name}", 1)
        elif format == "sql":
            content = content.replace(f"-- Rivet source: {auto_name}", f"-- Rivet source: {name}", 1)
        ext = "sql" if format == "sql" else "yaml"
        suggested_filename = f"{name}.{ext}"

    # Print to stdout (Req 12.4)
    if stdout:
        print(content, end="")
        return SUCCESS

    # Determine output path (Req 12.3)
    if output is not None:
        out_path = output
    else:
        out_path = os.path.join("sources", suggested_filename)

    # Write file (Req 12.1, 12.3)
    try:
        os.makedirs(os.path.dirname(out_path) if os.path.dirname(out_path) else ".", exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(content)
    except OSError as exc:
        err = CLIError(
            code="RVT-877",
            message=f"Failed to write output file '{out_path}': {exc}",
            remediation="Check file permissions and disk space.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return USAGE_ERROR

    print(render_generate_confirmation(source, out_path, globals.color))
    return SUCCESS
