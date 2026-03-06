"""SQL namespace preprocessor — single-pass table reference resolution and rewriting.

Tokenizes raw SQL, finds all table references, resolves each one, creates source
joints for catalog matches, and rewrites the SQL so every table reference is a
simple unqualified identifier.

Requirements: 1.1, 2.1, 2.2, 3.1, 3.4, 11.1, 11.2, 11.3
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlglot.tokens import Token, Tokenizer, TokenType  # type: ignore[attr-defined]

from rivet_core.interactive.types import PreprocessedSQL, ResolvedReference
from rivet_core.models import Joint

if TYPE_CHECKING:
    from rivet_core.catalog_explorer import CatalogExplorer

logger = logging.getLogger(__name__)

_TABLE_NODE_TYPES = frozenset({"table", "view", "file"})

_BACKTICK_RE = re.compile(r"`([^`]*)`")

# Token types that can appear as identifier parts in dotted table references.
# Includes VAR, IDENTIFIER, and SQL keywords that sqlglot tokenizes as their
# own type but can appear as schema/table names (e.g. DEFAULT in Unity Catalog).
_IDENT_TYPES = frozenset({
    TokenType.VAR,
    TokenType.IDENTIFIER,
    TokenType.DEFAULT,
    TokenType.REPLACE,
    TokenType.COMMENT,
    TokenType.BEGIN,
    TokenType.END,
    TokenType.EXECUTE,
    TokenType.FUNCTION,
    TokenType.PROCEDURE,
    TokenType.TEMPORARY,
    TokenType.RANGE,
    TokenType.ROWS,
    TokenType.SCHEMA,
    TokenType.TABLE,
    TokenType.VIEW,
    TokenType.INDEX,
    TokenType.COLUMN,
    TokenType.CONSTRAINT,
    TokenType.DESCRIBE,
    TokenType.FORMAT,
    TokenType.PIVOT,
    TokenType.UNPIVOT,
    TokenType.LOAD,
    TokenType.DIV,
    TokenType.FILTER,
    TokenType.NEXT,
    TokenType.FIRST,
    TokenType.OVERWRITE,
    TokenType.ROW,
    TokenType.MERGE,
    TokenType.SETTINGS,
    TokenType.KEEP,
    TokenType.KILL,
    TokenType.VOLATILE,
    TokenType.CURRENT_DATE,
    TokenType.CURRENT_DATETIME,
    TokenType.CURRENT_TIME,
    TokenType.CURRENT_TIMESTAMP,
    TokenType.CURRENT_USER,
    TokenType.COMMAND,
    TokenType.SHOW,
    TokenType.AUTO_INCREMENT,
    TokenType.COLLATE,
    TokenType.UNIQUE,
    TokenType.REFERENCES,
    TokenType.CACHE,
    TokenType.COMMIT,
    TokenType.ROLLBACK,
    TokenType.PERCENT,
    TokenType.SOME,
    TokenType.ALL,
    TokenType.ANY,
    TokenType.TRUE,
    TokenType.FALSE,
    TokenType.DATABASE,
    TokenType.DELETE,
    TokenType.UPDATE,
})

# Keywords after which the next identifier is in table position
_TABLE_POSITION_KEYWORDS = frozenset({
    TokenType.FROM,
    TokenType.JOIN,
    TokenType.INTO,
    TokenType.UPDATE,
    TokenType.TABLE,
})

# JOIN modifier keywords — after these, expect JOIN then table position
_JOIN_MODIFIERS = frozenset({
    TokenType.INNER,
    TokenType.LEFT,
    TokenType.RIGHT,
    TokenType.OUTER,
    TokenType.CROSS,
    TokenType.FULL,
    TokenType.NATURAL,
})

# Keywords that end table position context
_TABLE_POSITION_ENDERS = frozenset({
    TokenType.SELECT,
    TokenType.WHERE,
    TokenType.GROUP_BY,
    TokenType.ORDER_BY,
    TokenType.HAVING,
    TokenType.LIMIT,
    TokenType.UNION,
    TokenType.EXCEPT,
    TokenType.INTERSECT,
    TokenType.ON,
    TokenType.USING,
    TokenType.SET,
    TokenType.VALUES,
    TokenType.WINDOW,
    TokenType.RETURNING,
})


@dataclass(frozen=True)
class TableRefSpan:
    """A table reference found during token scanning."""

    ref_str: str
    parts: list[str]
    start_offset: int
    end_offset: int
    is_path_ref: bool


def _generate_source_name(
    base_name: str,
    joint_names: frozenset[str],
    existing_sources: set[str],
) -> str:
    """Generate a collision-free source joint name."""
    if "/" in base_name or "\\" in base_name:
        # Path: extract filename, strip extension
        filename = base_name.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
        sanitized = re.sub(r"\.[^.]+$", "", filename)
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", sanitized)
    else:
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", base_name)

    if not sanitized or sanitized[0].isdigit():
        sanitized = f"src_{sanitized}"

    if sanitized not in joint_names and sanitized not in existing_sources:
        return sanitized
    prefixed = f"__src_{sanitized}"
    if prefixed not in joint_names and prefixed not in existing_sources:
        return prefixed
    i = 2
    while f"{prefixed}_{i}" in joint_names or f"{prefixed}_{i}" in existing_sources:
        i += 1
    return f"{prefixed}_{i}"


def _apply_replacements(sql: str, replacements: list[tuple[int, int, str]]) -> str:
    """Apply replacements right-to-left to preserve offsets."""
    for start, end, text in sorted(replacements, key=lambda r: r[0], reverse=True):
        sql = sql[:start] + text + sql[end:]
    return sql


def _token_text_unquoted(token: Token) -> str:
    """Get the unquoted text value of a token."""
    return token.text


def _build_cte_names(tokens: list[Token]) -> set[str]:
    """Extract CTE names from WITH ... AS patterns."""
    cte_names: set[str] = set()
    i = 0
    n = len(tokens)
    while i < n:
        if tokens[i].token_type == TokenType.WITH:
            j = i + 1
            while j < n:
                if tokens[j].token_type in _IDENT_TYPES:
                    k = j + 1
                    if k < n and tokens[k].token_type == TokenType.ALIAS:
                        cte_names.add(_token_text_unquoted(tokens[j]))
                        # Skip past CTE body (parenthesized)
                        k += 1
                        depth = 0
                        while k < n:
                            if tokens[k].token_type == TokenType.L_PAREN:
                                depth += 1
                            elif tokens[k].token_type == TokenType.R_PAREN:
                                depth -= 1
                                if depth == 0:
                                    k += 1
                                    break
                            k += 1
                        if k < n and tokens[k].token_type == TokenType.COMMA:
                            j = k + 1
                            continue
                        break
                    else:
                        break
                else:
                    break
            break
        i += 1
    return cte_names


# CLEANUP-RISK: _scan_table_refs (complexity 20) — SQL AST traversal with dialect-specific logic; refactoring risks changing SQL parsing behavior
def _scan_table_refs(tokens: list[Token], sql: str) -> list[TableRefSpan]:
    """Scan tokens for table reference patterns in table positions."""
    cte_names = _build_cte_names(tokens)
    refs: list[TableRefSpan] = []
    in_table_position = False
    skip_next_ident = False  # skip alias after AS
    n = len(tokens)
    i = 0

    while i < n:
        tt = tokens[i].token_type

        if tt in _TABLE_POSITION_KEYWORDS:
            in_table_position = True
            skip_next_ident = False
            i += 1
            continue

        if tt in _JOIN_MODIFIERS:
            # Look ahead for JOIN
            j = i + 1
            if j < n and tokens[j].token_type in (TokenType.JOIN, TokenType.OUTER):
                in_table_position = True
                skip_next_ident = False
                i = j
                if tokens[j].token_type == TokenType.OUTER:
                    i += 1
                    if i < n and tokens[i].token_type == TokenType.JOIN:
                        i += 1
                    continue
                i += 1
                continue
            i += 1
            continue

        if tt == TokenType.COMMA and in_table_position:
            skip_next_ident = False
            i += 1
            continue

        if tt == TokenType.ALIAS:
            skip_next_ident = True
            i += 1
            continue

        if tt in _TABLE_POSITION_ENDERS:
            in_table_position = False
            skip_next_ident = False
            i += 1
            continue

        # Subquery starts a new scope
        if tt == TokenType.L_PAREN:
            # Don't reset table position — subqueries have their own FROM
            i += 1
            continue

        if not in_table_position:
            i += 1
            continue

        if skip_next_ident and tt in _IDENT_TYPES:
            skip_next_ident = False
            i += 1
            continue

        if tt in _IDENT_TYPES:
            # Collect dotted identifier sequence
            parts: list[str] = []
            start_offset = tokens[i].start
            end_offset = tokens[i].end
            parts.append(_token_text_unquoted(tokens[i]))
            j = i + 1
            is_path_ref = False

            while j + 1 < n and tokens[j].token_type == TokenType.DOT:
                next_tok = tokens[j + 1]
                if next_tok.token_type in _IDENT_TYPES:
                    # Check for path ref: IDENT."quoted" where IDENTIFIER has quotes
                    if next_tok.token_type == TokenType.IDENTIFIER and len(parts) == 1:
                        raw = sql[next_tok.start : next_tok.end + 1]
                        if raw.startswith('"') or raw.startswith("'"):
                            is_path_ref = True
                    parts.append(_token_text_unquoted(next_tok))
                    end_offset = next_tok.end
                    j += 2
                else:
                    break

            # Skip CTE name refs
            if len(parts) == 1 and parts[0] in cte_names:
                i = j
                continue

            refs.append(TableRefSpan(
                ref_str=".".join(parts),
                parts=parts,
                start_offset=start_offset,
                end_offset=end_offset + 1,  # exclusive
                is_path_ref=is_path_ref,
            ))
            # After a table ref, next single identifier is an implicit alias
            # (unless it's a keyword that changes context)
            if j < n and tokens[j].token_type in _IDENT_TYPES:
                # Implicit alias — skip it
                j += 1
            i = j
            continue

        i += 1

    return refs


def _resolve_ref(
    ref_str: str,
    parts: list[str],
    is_path_ref: bool,
    joint_names: frozenset[str],
    catalog_names: frozenset[str],
    catalog_context: str | None,
    cached_joints: frozenset[str] | None,
    catalog_explorer: CatalogExplorer | None,
) -> tuple[ResolvedReference, str | None]:
    """Resolve a single reference.

    Returns (resolution, replacement_base_name).
    replacement_base_name is None for joint matches (no replacement needed).
    """
    cached = cached_joints or frozenset()

    if is_path_ref and len(parts) == 2:
        cat_name, path_str = parts
        if cat_name in catalog_names:
            return (
                ResolvedReference(
                    kind="catalog_table", joint_name=None,
                    catalog=cat_name, schema=None, table=path_str, cached=False,
                ),
                path_str,
            )
        # Unknown catalog prefix for path — pass through
        return (
            ResolvedReference(
                kind="catalog_table", joint_name=None,
                catalog=None, schema=None, table=ref_str, cached=False,
            ),
            None,
        )

    n = len(parts)

    if n == 1:
        name = parts[0]
        if name in joint_names:
            return (
                ResolvedReference(
                    kind="joint", joint_name=name,
                    catalog=None, schema=None, table=None, cached=name in cached,
                ),
                None,
            )
        if catalog_context is not None:
            return (
                ResolvedReference(
                    kind="catalog_table", joint_name=None,
                    catalog=catalog_context, schema=None, table=name, cached=False,
                ),
                name,
            )
        if catalog_explorer is not None:
            return _fuzzy_resolve(name, catalog_explorer), name
        raise ValueError(
            f"Cannot resolve '{name}': not a known joint and no catalog context set."
        )

    if n == 2:
        first, second = parts
        if first in catalog_names:
            return (
                ResolvedReference(
                    kind="catalog_table", joint_name=None,
                    catalog=first, schema=None, table=second, cached=False,
                ),
                second,
            )
        if catalog_context is not None:
            return (
                ResolvedReference(
                    kind="catalog_table", joint_name=None,
                    catalog=catalog_context, schema=first, table=second, cached=False,
                ),
                second,
            )
        raise ValueError(
            f"Cannot resolve '{ref_str}': '{first}' is not a known catalog."
        )

    if n == 3:
        catalog, schema, table = parts
        return (
            ResolvedReference(
                kind="catalog_table", joint_name=None,
                catalog=catalog, schema=schema, table=table, cached=False,
            ),
            table,
        )

    # 4+ parts
    first = parts[0]
    if first not in catalog_names:
        known = ", ".join(sorted(catalog_names))
        raise ValueError(
            f"Cannot resolve '{ref_str}': '{first}' is not a known catalog. "
            f"Known catalogs: {known}"
        )
    remaining = parts[1:]
    schema_part = ".".join(remaining[:-1]) if len(remaining) > 1 else None
    table_part = remaining[-1]
    return (
        ResolvedReference(
            kind="catalog_table", joint_name=None,
            catalog=first, schema=schema_part, table=table_part, cached=False,
        ),
        table_part,
    )


def _fuzzy_resolve(name: str, explorer: CatalogExplorer) -> ResolvedReference:
    """Search cached catalog metadata for a table matching name."""
    candidates: list[tuple[str, str, str]] = []
    for catalog_name, nodes in explorer._tables_cache.items():
        for node in nodes:
            if node.node_type in _TABLE_NODE_TYPES and node.name == name:
                schema = node.path[-2] if len(node.path) >= 2 else None
                candidates.append((catalog_name, schema or "", node.name))

    if len(candidates) == 1:
        cat, sch, tbl = candidates[0]
        return ResolvedReference(
            kind="catalog_table", joint_name=None,
            catalog=cat, schema=sch or None, table=tbl, cached=False,
        )

    if len(candidates) > 1:
        fqns = [f"{c}.{s}.{t}" for c, s, t in candidates]
        raise ValueError(
            f"Cannot resolve '{name}': matches {', '.join(fqns)}. "
            "Qualify the reference to disambiguate."
        )

    connected = sorted(
        cat_name
        for cat_name, (ok, _) in explorer._connection_status.items()
        if ok
    )
    raise ValueError(
        f"Cannot resolve '{name}': no matching table found in cached metadata. "
        f"Connected catalogs: {', '.join(connected) if connected else '(none)'}"
    )


def preprocess_sql(
    sql: str,
    joint_names: frozenset[str],
    catalog_names: frozenset[str],
    catalog_context: str | None,
    cached_joints: frozenset[str] | None = None,
    catalog_explorer: CatalogExplorer | None = None,
) -> PreprocessedSQL:
    """Preprocess SQL: resolve all table refs and rewrite to simple identifiers."""
    # Normalize backtick-quoted identifiers to double-quoted (Req 4.5)
    sql = _BACKTICK_RE.sub(r'"\1"', sql)
    try:
        tokens = list(Tokenizer().tokenize(sql))
    except Exception:
        logger.warning("Failed to tokenize SQL, returning unchanged")
        return PreprocessedSQL(sql=sql, source_joints=[], resolved_refs={})

    if not tokens:
        return PreprocessedSQL(sql=sql, source_joints=[], resolved_refs={})

    spans = _scan_table_refs(tokens, sql)
    if not spans:
        return PreprocessedSQL(sql=sql, source_joints=[], resolved_refs={})

    resolved_refs: dict[str, ResolvedReference] = {}
    source_joint_map: dict[str, Joint] = {}  # dedup_key -> Joint (Property 7)
    source_names: set[str] = set()
    ref_to_source: dict[str, str] = {}  # ref_str -> source joint name
    replacements: list[tuple[int, int, str]] = []

    for span in spans:
        if span.ref_str in resolved_refs:
            # Already resolved — reuse replacement
            if span.ref_str in ref_to_source:
                replacements.append((
                    span.start_offset, span.end_offset, ref_to_source[span.ref_str]
                ))
            continue

        resolution, replacement_base = _resolve_ref(
            span.ref_str, span.parts, span.is_path_ref,
            joint_names, catalog_names, catalog_context,
            cached_joints, catalog_explorer,
        )
        resolved_refs[span.ref_str] = resolution

        if resolution.kind == "joint" or replacement_base is None:
            continue

        # Deduplicate same catalog table (Property 7)
        dedup_key = f"{resolution.catalog}|{resolution.schema}|{resolution.table}"
        if dedup_key in source_joint_map:
            source_name = source_joint_map[dedup_key].name
        else:
            source_name = _generate_source_name(replacement_base, joint_names, source_names)
            source_names.add(source_name)

            table_ref = resolution.table
            if resolution.schema:
                table_ref = f"{resolution.schema}.{resolution.table}"

            source_joint_map[dedup_key] = Joint(
                name=source_name,
                joint_type="source",
                catalog=resolution.catalog,
                table=table_ref,
            )

        ref_to_source[span.ref_str] = source_name
        replacements.append((span.start_offset, span.end_offset, source_name))

    rewritten = _apply_replacements(sql, replacements) if replacements else sql
    return PreprocessedSQL(
        sql=rewritten,
        source_joints=list(source_joint_map.values()),
        resolved_refs=resolved_refs,
    )
