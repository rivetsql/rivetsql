"""JSON renderer for catalog explorer CLI commands."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from rivet_core.catalog_explorer import CatalogInfo, ExplorerNode, NodeDetail, SearchResult


def _node_to_dict(node: ExplorerNode) -> dict[str, Any]:
    return {
        "name": node.name,
        "node_type": node.node_type,
        "path": node.path,
        "is_expandable": node.is_expandable,
        "depth": node.depth,
    }


def render_list_json(
    catalogs: list[CatalogInfo],
    children: dict[str, list[ExplorerNode]] | None = None,
) -> str:
    """Render list output as JSON: {"catalogs": [{name, type, connected, children}]}.

    Requirements: 9.4
    """
    result: list[dict[str, Any]] = []
    for cat in catalogs:
        entry: dict[str, Any] = {
            "name": cat.name,
            "type": cat.catalog_type,
            "connected": cat.connected,
        }
        if children is not None and cat.name in children:
            entry["children"] = [_node_to_dict(n) for n in children[cat.name]]
        else:
            entry["children"] = []
        result.append(entry)
    return json.dumps({"catalogs": result}, indent=2)


def render_describe_json(detail: NodeDetail) -> str:
    """Render describe output as JSON: {columns: [...], metadata: {...}}.

    Requirements: 10.3
    """
    columns: list[dict[str, Any]] = []
    if detail.schema is not None:
        for col in detail.schema.columns:
            col_dict: dict[str, Any] = {
                "name": col.name,
                "type": col.type,
            }
            if hasattr(col, "nullable"):
                col_dict["nullable"] = col.nullable
            if hasattr(col, "default"):
                col_dict["default"] = col.default
            if hasattr(col, "constraints"):
                col_dict["constraints"] = col.constraints
            columns.append(col_dict)

    metadata: dict[str, Any] = {}
    if detail.metadata is not None:
        metadata = asdict(detail.metadata)

    return json.dumps({"columns": columns, "metadata": metadata}, indent=2)


def render_search_json(results: list[SearchResult]) -> str:
    """Render search results as JSON array: [{qualified_name, kind, score, match_positions}].

    Requirements: 11.3
    """
    output = [
        {
            "qualified_name": r.qualified_name,
            "kind": r.kind,
            "score": r.score,
            "match_positions": r.match_positions,
        }
        for r in results
    ]
    return json.dumps(output, indent=2)


def render_children_json(
    nodes: list[ExplorerNode],
    children_fn: object | None = None,
    depth: int = 0,
) -> str:
    """Render a list of ExplorerNode objects as a JSON array.

    Each node is serialized with ``name``, ``node_type``, ``path``, and
    ``is_expandable`` fields.  When *depth* > 0 and *children_fn* is provided,
    expandable nodes include a nested ``children`` array (recursively, up to
    *depth* levels).

    Requirements: 3.3, 3.4
    """

    def _node_with_children(node: ExplorerNode, remaining: int) -> dict[str, Any]:
        entry: dict[str, Any] = {
            "name": node.name,
            "node_type": node.node_type,
            "path": node.path,
            "is_expandable": node.is_expandable,
        }
        if remaining > 0 and node.is_expandable and children_fn is not None:
            kids = children_fn(node.path)  # type: ignore[operator]
            entry["children"] = [_node_with_children(k, remaining - 1) for k in kids]
        return entry

    result = [_node_with_children(n, depth) for n in nodes]
    return json.dumps(result, indent=2)
