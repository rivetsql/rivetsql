"""Text format renderer for catalog explorer commands."""

from __future__ import annotations

from rivet_cli.rendering.colors import (
    BOLD,
    CYAN,
    DIM,
    GREEN,
    RED,
    YELLOW,
    colorize,
)
from rivet_core.catalog_explorer import (
    CatalogInfo,
    ExplorerNode,
    GeneratedSource,
    NodeDetail,
    SearchResult,
)

# Tree symbols
_SYM_CONNECTED = "●"
_SYM_DISCONNECTED = "○"
_SYM_EXPAND = "▶"
_SYM_COLLAPSE = "▼"
_SYM_LEAF = "·"


def render_catalog_list(catalogs: list[CatalogInfo], color: bool) -> str:
    """Render list_catalogs() output as a text table (name, type, status).

    Requirements: 9.1
    """
    if not catalogs:
        return colorize("No catalogs configured.", DIM, color)

    name_w = max(len(c.name) for c in catalogs)
    type_w = max(len(c.catalog_type) for c in catalogs)
    name_w = max(name_w, 4)  # "Name"
    type_w = max(type_w, 4)  # "Type"

    header = (
        colorize(f"{'Name':<{name_w}}", BOLD, color)
        + "  "
        + colorize(f"{'Type':<{type_w}}", BOLD, color)
        + "  "
        + colorize("Status", BOLD, color)
    )
    sep = "-" * (name_w + 2 + type_w + 2 + 12)
    lines = [header, sep]

    for c in catalogs:
        if c.connected:
            status = colorize(f"{_SYM_CONNECTED} connected", GREEN, color)
        else:
            err = f" ({c.error})" if c.error else ""
            status = colorize(f"{_SYM_DISCONNECTED} disconnected{err}", RED, color)
        lines.append(f"{c.name:<{name_w}}  {c.catalog_type:<{type_w}}  {status}")

    return "\n".join(lines)


def render_catalog_tree(
    catalogs: list[CatalogInfo],
    children_fn: object,  # Callable[[list[str]], list[ExplorerNode]]
    depth: int,
    color: bool,
) -> str:
    """Render catalog tree with indentation and expand/collapse indicators.

    Requirements: 9.5
    """
    lines: list[str] = []

    for cat in catalogs:
        _render_catalog_node(lines, cat, children_fn, depth, color)

    return "\n".join(lines) if lines else colorize("No catalogs configured.", DIM, color)


def render_catalog_tree_from_nodes(
    nodes: list[ExplorerNode],
    children_fn: object,
    depth: int,
    color: bool,
) -> str:
    """Render a list of ExplorerNode objects as a tree.

    Thin entry point for path-based listing — reuses ``_render_explorer_node``
    to render children of an arbitrary target node rather than starting from
    ``CatalogInfo`` objects.

    Requirements: 3.1, 3.2
    """
    lines: list[str] = []
    for node in nodes:
        _render_explorer_node(lines, node, children_fn, depth, color, indent=0)
    return "\n".join(lines) if lines else colorize("No children.", DIM, color)


def _render_catalog_node(
    lines: list[str],
    cat: CatalogInfo,
    children_fn: object,
    remaining_depth: int,
    color: bool,
) -> None:
    if cat.connected:
        sym = colorize(_SYM_CONNECTED, GREEN, color)
    else:
        sym = colorize(_SYM_DISCONNECTED, RED, color)

    expand_sym = _SYM_COLLAPSE if (cat.connected and remaining_depth > 0) else _SYM_EXPAND
    name = colorize(cat.name, BOLD, color)
    type_label = colorize(f"({cat.catalog_type})", DIM, color)
    lines.append(f"{expand_sym} {sym} {name} {type_label}")

    if cat.connected and remaining_depth > 0:
        children = children_fn([cat.name])  # type: ignore[operator]
        for node in children:
            _render_explorer_node(lines, node, children_fn, remaining_depth - 1, color, indent=2)


def _render_explorer_node(
    lines: list[str],
    node: ExplorerNode,
    children_fn: object,
    remaining_depth: int,
    color: bool,
    indent: int,
) -> None:
    prefix = " " * indent
    if node.is_expandable and remaining_depth > 0:
        sym = _SYM_COLLAPSE
    elif node.is_expandable:
        sym = _SYM_EXPAND
    else:
        sym = _SYM_LEAF

    name = colorize(node.name, CYAN if node.node_type in ("table", "view", "file") else "", color)
    lines.append(f"{prefix}{sym} {name}")

    if node.is_expandable and remaining_depth > 0 and not node.depth_limit_reached:
        children = children_fn(node.path)  # type: ignore[operator]
        for child in children:
            _render_explorer_node(lines, child, children_fn, remaining_depth - 1, color, indent + 2)


def render_node_detail(detail: NodeDetail, color: bool, show_stats: bool = False) -> str:
    """Render describe output: column table + metadata section.

    Requirements: 10.1, 11.1
    """
    lines: list[str] = []
    node = detail.node
    path_str = ".".join(node.path)
    lines.append(colorize(f"Table: {path_str}", BOLD, color))
    lines.append("")

    # Column table
    if detail.schema and detail.schema.columns:
        cols = detail.schema.columns
        name_w = max(len(c.name) for c in cols)
        type_w = max(len(c.type) for c in cols)
        name_w = max(name_w, 4)
        type_w = max(type_w, 4)

        header = (
            colorize(f"{'Name':<{name_w}}", BOLD, color)
            + "  "
            + colorize(f"{'Type':<{type_w}}", BOLD, color)
            + "  "
            + colorize(f"{'Nullable':<8}", BOLD, color)
            + "  "
            + colorize("Default", BOLD, color)
        )
        sep = "-" * (name_w + 2 + type_w + 2 + 8 + 2 + 12)
        lines.append(header)
        lines.append(sep)

        for col in cols:
            nullable = "yes" if col.nullable else "no"
            default = col.default or ""
            lines.append(f"{col.name:<{name_w}}  {col.type:<{type_w}}  {nullable:<8}  {default}")
    else:
        lines.append(colorize("(no schema available)", DIM, color))

    # Metadata section
    if detail.metadata:
        lines.append("")
        lines.append(colorize("Metadata", BOLD, color))
        lines.append("-" * 20)
        m = detail.metadata
        if m.row_count is not None:
            lines.append(f"  Rows:          {m.row_count:,}")
        if m.size_bytes is not None:
            lines.append(f"  Size:          {_fmt_bytes(m.size_bytes)}")
        if m.format:
            lines.append(f"  Format:        {m.format}")
        if m.last_modified:
            lines.append(f"  Last modified: {m.last_modified.isoformat()}")
        if m.created_at:
            lines.append(f"  Created:       {m.created_at.isoformat()}")

    return "\n".join(lines)


def render_search_results(results: list[SearchResult], color: bool) -> str:
    """Render search results as a ranked list with qualified names and node types.

    Requirements: 11.1
    """
    if not results:
        return colorize("No results found.", DIM, color)

    lines: list[str] = []
    for i, r in enumerate(results, 1):
        rank = colorize(f"{i:>3}.", DIM, color)
        kind = colorize(f"[{r.kind}]", YELLOW, color)
        name = colorize(r.qualified_name, BOLD, color)
        lines.append(f"{rank} {kind} {name}")

    return "\n".join(lines)


def render_generate_confirmation(source: GeneratedSource, output_path: str, color: bool) -> str:
    """Render generate confirmation message.

    Requirements: 12.1
    """
    lines = [
        colorize("Generated source joint declaration", GREEN, color),
        f"  Table:    {source.catalog_name}.{source.table_name}",
        f"  Format:   {source.format}",
        f"  Columns:  {source.column_count}",
        f"  Written:  {output_path}",
    ]
    return "\n".join(lines)


def _fmt_bytes(n: int) -> str:
    """Format byte count as human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n //= 1024
    return f"{n:.1f} PB"
