"""Builds transient mini-pipelines from ad-hoc SQL.

Implements the "Everything is a Joint" principle: every REPL query is modeled
as a transient pipeline that reuses project joints directly and goes through
the standard Rivet compilation/execution path.

Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 11.1, 11.3, 11.4, 11.5
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rivet_core.assembly import Assembly
from rivet_core.compiler import CompiledAssembly
from rivet_core.interactive.material_cache import MaterialCache
from rivet_core.interactive.sql_preprocessor import preprocess_sql
from rivet_core.interactive.types import ResolvedReference
from rivet_core.models import Joint
from rivet_core.sql_parser import SQLParser

if TYPE_CHECKING:
    from rivet_core.catalog_explorer import CatalogExplorer

_TABLE_NODE_TYPES = frozenset({"table", "view", "file"})


class QueryPlanner:
    """Builds transient mini-pipelines from ad-hoc SQL."""

    def __init__(self, catalog_explorer: CatalogExplorer | None = None) -> None:
        self._parser = SQLParser()
        self._catalog_explorer = catalog_explorer

    # CLEANUP-RISK: build_transient_pipeline (complexity 19) — interactive pipeline construction with multiple fallback paths; refactoring risks changing REPL behavior
    def build_transient_pipeline(
        self,
        sql: str,
        catalog_context: str | None,
        assembly: CompiledAssembly,
        material_cache: MaterialCache,
        catalog_names: frozenset[str],
        raw_assembly: Assembly | None = None,
        engine_override: str | None = None,
    ) -> tuple[Assembly, list[str]]:
        """Parse SQL, resolve table refs, build transient Assembly.

        Delegates all reference resolution and SQL rewriting to preprocess_sql.
        Then merges results into a transient Assembly with upstream closure.

        Returns:
            (transient Assembly, list of joint names needing execution)
        """
        cached_joints = frozenset(material_cache._cache)
        compiled_map = {j.name: j for j in assembly.joints}
        raw_joints: dict[str, Joint] = {}
        if raw_assembly is not None:
            try:
                joints_attr = raw_assembly.joints
                if isinstance(joints_attr, dict):
                    raw_joints = joints_attr
            except (AttributeError, TypeError):
                pass

        # 1. Preprocess: resolve all table refs and rewrite SQL
        result = preprocess_sql(
            sql=sql,
            joint_names=frozenset(j.name for j in assembly.joints),
            catalog_names=catalog_names,
            catalog_context=catalog_context,
            cached_joints=cached_joints,
            catalog_explorer=self._catalog_explorer,
        )

        # 2. Parse clean SQL (simple identifiers only)
        self._parser.parse(result.sql)

        # 3. Merge source joints
        included_joints: dict[str, Joint] = {}
        for sj in result.source_joints:
            if engine_override:
                sj = Joint(
                    name=sj.name, joint_type=sj.joint_type,
                    catalog=sj.catalog, table=sj.table, engine=engine_override,
                )
            included_joints[sj.name] = sj

        # 4. Include upstream closure for joint refs
        needs_execution: list[str] = []
        for _ref_str, resolution in result.resolved_refs.items():
            if resolution.kind != "joint" or not resolution.joint_name:
                continue
            closure = self._upstream_closure(resolution.joint_name, assembly)
            for jn in closure:
                if jn in included_joints:
                    continue
                raw = raw_joints.get(jn)
                if raw:
                    included_joints[jn] = raw
                else:
                    cj = compiled_map[jn]
                    included_joints[jn] = Joint(
                        name=cj.name,
                        joint_type=cj.type,
                        catalog=cj.catalog,
                        upstream=list(cj.upstream),
                        sql=cj.sql,
                        table=cj.table,
                        engine=cj.engine,
                        path=None,
                        source_file=cj.source_file,
                        dialect=cj.sql_dialect,
                    )
            if not resolution.cached:
                needs_execution.extend(
                    jn for jn in closure
                    if jn not in cached_joints
                )

        # 5. Build __query joint with rewritten SQL
        upstream_names: list[str] = []
        seen_upstream: set[str] = set()
        for _ref_str, resolution in result.resolved_refs.items():
            if resolution.kind == "joint" and resolution.joint_name:
                name = resolution.joint_name
            else:
                # Find the source joint name for this ref
                name = next(  # type: ignore[assignment]
                    (sj.name for sj in result.source_joints
                     if sj.catalog == resolution.catalog and
                     sj.table == (f"{resolution.schema}.{resolution.table}" if resolution.schema else resolution.table)),
                    None,
                )
            if name and name in included_joints and name not in seen_upstream:
                upstream_names.append(name)
                seen_upstream.add(name)
        # Ensure all source joints are in upstream
        for sj in result.source_joints:
            if sj.name not in seen_upstream:
                upstream_names.append(sj.name)
                seen_upstream.add(sj.name)

        query_joint = Joint(
            name="__query",
            joint_type="sql",
            sql=result.sql,
            upstream=upstream_names,
            engine=engine_override,
        )
        included_joints["__query"] = query_joint

        # Build the transient assembly
        all_joints = list(included_joints.values())
        transient_assembly = Assembly(all_joints)

        # Deduplicate needs_execution
        seen: set[str] = set()
        unique_needs: list[str] = []
        for name in needs_execution:
            if name not in seen:
                seen.add(name)
                unique_needs.append(name)

        return transient_assembly, unique_needs

    def _upstream_closure(
        self, joint_name: str, assembly: CompiledAssembly
    ) -> list[str]:
        """Return joint_name and all transitive upstream deps in topological order."""
        joint_map = {j.name: j for j in assembly.joints}
        if joint_name not in joint_map:
            return [joint_name]

        visited: set[str] = set()
        order: list[str] = []

        def visit(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            joint = joint_map.get(name)
            if joint:
                for up in joint.upstream:
                    visit(up)
            order.append(name)

        visit(joint_name)
        return order

    @staticmethod
    def _resolve_ref(
        table_ref: str,
        assembly: CompiledAssembly,
        catalog_context: str | None,
        cached_joints: set[str] | None = None,
        catalog_explorer: CatalogExplorer | None = None,
    ) -> ResolvedReference:
        """Resolve a table reference to a joint or catalog table."""
        cached_joints = cached_joints or set()
        joint_names = {j.name for j in assembly.joints}
        catalog_names = {c.name for c in assembly.catalogs}
        parts = table_ref.split(".")

        if len(parts) == 1:
            name = parts[0]
            if name in joint_names:
                return ResolvedReference(
                    kind="joint", joint_name=name, catalog=None,
                    schema=None, table=None, cached=name in cached_joints,
                )
            if catalog_context is not None:
                return ResolvedReference(
                    kind="catalog_table", joint_name=None, catalog=catalog_context,
                    schema=None, table=name, cached=False,
                )
            if catalog_explorer is not None:
                return QueryPlanner._fuzzy_resolve(name, catalog_explorer)
            return ResolvedReference(
                kind="catalog_table", joint_name=None, catalog=None,
                schema=None, table=name, cached=False,
            )

        if len(parts) == 2:
            first, second = parts
            if first in catalog_names:
                return ResolvedReference(
                    kind="catalog_table", joint_name=None, catalog=first,
                    schema=None, table=second, cached=False,
                )
            return ResolvedReference(
                kind="catalog_table", joint_name=None, catalog=catalog_context,
                schema=first, table=second, cached=False,
            )

        if len(parts) == 3:
            return ResolvedReference(
                kind="catalog_table", joint_name=None, catalog=parts[0],
                schema=parts[1], table=parts[2], cached=False,
            )

        # 4+ parts: last 3 as catalog.schema.table
        return ResolvedReference(
            kind="catalog_table", joint_name=None, catalog=parts[-3],
            schema=parts[-2], table=parts[-1], cached=False,
        )

    @staticmethod
    def _fuzzy_resolve(
        name: str, explorer: CatalogExplorer,
    ) -> ResolvedReference:
        """Search cached catalog metadata for a table matching *name*."""
        candidates: list[tuple[str, str, str]] = []
        for catalog_name, nodes in explorer._tables_cache.items():
            for node in nodes:
                if node.node_type in _TABLE_NODE_TYPES and node.name == name:
                    schema = node.path[-2] if len(node.path) >= 2 else None
                    candidates.append((catalog_name, schema or "", node.name))

        if len(candidates) == 1:
            cat, sch, tbl = candidates[0]
            return ResolvedReference(
                kind="catalog_table", joint_name=None, catalog=cat,
                schema=sch or None, table=tbl, cached=False,
            )

        if len(candidates) > 1:
            fqns = [f"{c}.{s}.{t}" for c, s, t in candidates]
            raise ValueError(
                f"Ambiguous reference '{name}': matches {', '.join(fqns)}"
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
