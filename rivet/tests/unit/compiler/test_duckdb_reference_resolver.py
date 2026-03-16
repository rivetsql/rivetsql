"""Unit tests for DuckDBReferenceResolver.

Verifies that the resolver rewrites logical source names in fused SQL
into DuckDB reader functions (filesystem) or table references (DuckDB)
at compile time, respecting CTE siblings and word boundaries.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from rivet_duckdb.engine import DuckDBReferenceResolver

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _joint(
    name: str,
    *,
    type: str = "transform",
    upstream: list[str] | None = None,
    sql: str | None = None,
    sql_translated: str | None = None,
    catalog: str | None = None,
    table: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        type=type,
        upstream=upstream or [],
        sql=sql,
        sql_translated=sql_translated,
        catalog=catalog,
        table=table,
    )


def _fs_catalog(path: str, *, fmt: str | None = None) -> SimpleNamespace:
    """Create a filesystem catalog stub."""
    opts: dict[str, Any] = {"path": path, "type": "filesystem"}
    if fmt is not None:
        opts["format"] = fmt
    return SimpleNamespace(type="filesystem", options=opts)


def _duckdb_catalog() -> SimpleNamespace:
    """Create a DuckDB catalog stub."""
    return SimpleNamespace(type="duckdb", options={"type": "duckdb"})


# ---------------------------------------------------------------------------
# 3.2 — Filesystem: CSV, Parquet, JSON, stem fallback, format catalogue
# ---------------------------------------------------------------------------


class TestFilesystemCSV:
    def test_filesystem_csv_source_resolved(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "products.csv"
        csv_file.write_text("id,name\n1,Widget\n")

        source = _joint("src_products", type="source", catalog="fs", table="products.csv")
        query = _joint("q", upstream=["src_products"], sql="SELECT * FROM src_products")
        compiled: dict[str, Any] = {"src_products": source, "q": query}
        catalog_map: dict[str, Any] = {"fs": _fs_catalog(str(tmp_path))}

        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT * FROM src_products",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["src_products", "q"],
        )

        assert result is not None
        assert f"read_csv_auto('{csv_file}')" in result


class TestFilesystemParquet:
    def test_filesystem_parquet_source_resolved(self, tmp_path: Path) -> None:
        pq_file = tmp_path / "orders.parquet"
        pq_file.write_bytes(b"PAR1")  # dummy content

        source = _joint("src_orders", type="source", catalog="fs", table="orders.parquet")
        query = _joint("q", upstream=["src_orders"], sql="SELECT * FROM src_orders")
        compiled: dict[str, Any] = {"src_orders": source, "q": query}
        catalog_map: dict[str, Any] = {"fs": _fs_catalog(str(tmp_path))}

        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT * FROM src_orders",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["src_orders", "q"],
        )

        assert result is not None
        assert f"read_parquet('{pq_file}')" in result


class TestFilesystemJSON:
    def test_filesystem_json_source_resolved(self, tmp_path: Path) -> None:
        json_file = tmp_path / "events.json"
        json_file.write_text('[{"id": 1}]')

        source = _joint("src_events", type="source", catalog="fs", table="events.json")
        query = _joint("q", upstream=["src_events"], sql="SELECT * FROM src_events")
        compiled: dict[str, Any] = {"src_events": source, "q": query}
        catalog_map: dict[str, Any] = {"fs": _fs_catalog(str(tmp_path))}

        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT * FROM src_events",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["src_events", "q"],
        )

        assert result is not None
        assert f"read_json_auto('{json_file}')" in result


class TestFilesystemStemFallback:
    def test_filesystem_stem_fallback(self, tmp_path: Path) -> None:
        """Table name 'products' without extension → finds products.csv by stem."""
        csv_file = tmp_path / "products.csv"
        csv_file.write_text("id,name\n1,Widget\n")

        source = _joint("src_products", type="source", catalog="fs", table="products")
        query = _joint("q", upstream=["src_products"], sql="SELECT * FROM src_products")
        compiled: dict[str, Any] = {"src_products": source, "q": query}
        catalog_map: dict[str, Any] = {"fs": _fs_catalog(str(tmp_path))}

        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT * FROM src_products",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["src_products", "q"],
        )

        assert result is not None
        assert f"read_csv_auto('{csv_file}')" in result


class TestFilesystemFormatFromCatalog:
    def test_filesystem_format_from_catalog_option(self, tmp_path: Path) -> None:
        """Explicit format in catalog takes priority over file extension."""
        # File has .csv extension but catalog says parquet
        data_file = tmp_path / "data.csv"
        data_file.write_text("dummy")

        source = _joint("src_data", type="source", catalog="fs", table="data.csv")
        query = _joint("q", upstream=["src_data"], sql="SELECT * FROM src_data")
        compiled: dict[str, Any] = {"src_data": source, "q": query}
        catalog_map: dict[str, Any] = {"fs": _fs_catalog(str(tmp_path), fmt="parquet")}

        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT * FROM src_data",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["src_data", "q"],
        )

        assert result is not None
        assert f"read_parquet('{data_file}')" in result


# ---------------------------------------------------------------------------
# 3.3 — DuckDB: simple name, qualified name
# ---------------------------------------------------------------------------


class TestDuckDBSimpleTable:
    def test_duckdb_source_simple_table(self) -> None:
        source = _joint("src_users", type="source", catalog="db", table="users")
        query = _joint("q", upstream=["src_users"], sql="SELECT * FROM src_users")
        compiled: dict[str, Any] = {"src_users": source, "q": query}
        catalog_map: dict[str, Any] = {"db": _duckdb_catalog()}

        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT * FROM src_users",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["src_users", "q"],
        )

        assert result is not None
        assert result == "SELECT * FROM users"


class TestDuckDBQualifiedTable:
    def test_duckdb_source_qualified_table(self) -> None:
        source = _joint("src_orders", type="source", catalog="db", table="main.orders")
        query = _joint("q", upstream=["src_orders"], sql="SELECT * FROM src_orders")
        compiled: dict[str, Any] = {"src_orders": source, "q": query}
        catalog_map: dict[str, Any] = {"db": _duckdb_catalog()}

        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT * FROM src_orders",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["src_orders", "q"],
        )

        assert result is not None
        assert result == "SELECT * FROM main.orders"


# ---------------------------------------------------------------------------
# 3.4 — CTE siblings: not rewritten
# ---------------------------------------------------------------------------


class TestCTESiblingNotRewritten:
    def test_cte_sibling_not_rewritten(self) -> None:
        """A transform joint with SQL in the same fused group is a CTE sibling
        and must NOT be rewritten."""
        source = _joint("raw", type="source", catalog="db", table="raw_table")
        transform = _joint(
            "clean",
            type="transform",
            upstream=["raw"],
            sql="SELECT id FROM raw",
        )
        final = _joint(
            "final",
            type="transform",
            upstream=["clean"],
            sql="SELECT * FROM clean",
        )
        compiled: dict[str, Any] = {"raw": source, "clean": transform, "final": final}
        catalog_map: dict[str, Any] = {"db": _duckdb_catalog()}
        fused = ["raw", "clean", "final"]

        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT * FROM clean",
            joint=final,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=fused,
        )

        # 'clean' is a transform with SQL → CTE sibling → not rewritten
        assert result is None


# ---------------------------------------------------------------------------
# 3.5 — Word boundary: no partial replacement
# ---------------------------------------------------------------------------


class TestWordBoundaryNoPartialReplace:
    def test_word_boundary_no_partial_replace(self, tmp_path: Path) -> None:
        """src_products must not replace src_products_v2."""
        csv_file = tmp_path / "products.csv"
        csv_file.write_text("id,name\n1,Widget\n")

        source = _joint("src_products", type="source", catalog="fs", table="products.csv")
        query = _joint(
            "q",
            upstream=["src_products"],
            sql="SELECT * FROM src_products JOIN src_products_v2 ON src_products.id = src_products_v2.id",
        )
        compiled: dict[str, Any] = {"src_products": source, "q": query}
        catalog_map: dict[str, Any] = {"fs": _fs_catalog(str(tmp_path))}

        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT * FROM src_products JOIN src_products_v2 ON src_products.id = src_products_v2.id",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["src_products", "q"],
        )

        assert result is not None
        reader = f"read_csv_auto('{csv_file}')"
        # src_products replaced
        assert reader in result
        # src_products_v2 NOT replaced
        assert "src_products_v2" in result
        assert f"{reader}_v2" not in result


# ---------------------------------------------------------------------------
# 3.6 — Errors: file not found, unknown format, no path, no upstream
# ---------------------------------------------------------------------------


class TestNoUpstreamReturnsNone:
    def test_no_upstream_returns_none(self) -> None:
        joint = _joint("lonely", upstream=[])
        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT 1",
            joint=joint,
            catalog=None,
            compiled_joints={},
            catalog_map={},
        )
        assert result is None


class TestMissingFileReturnsNone:
    def test_missing_file_returns_none(self, tmp_path: Path) -> None:
        """File not found → source ignored, returns None."""
        source = _joint("src_missing", type="source", catalog="fs", table="nonexistent.csv")
        query = _joint("q", upstream=["src_missing"], sql="SELECT * FROM src_missing")
        compiled: dict[str, Any] = {"src_missing": source, "q": query}
        catalog_map: dict[str, Any] = {"fs": _fs_catalog(str(tmp_path))}

        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT * FROM src_missing",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["src_missing", "q"],
        )

        assert result is None


class TestUnknownFormatIgnored:
    def test_unknown_format_ignored(self, tmp_path: Path) -> None:
        """Extension .xyz → source ignored."""
        weird_file = tmp_path / "data.xyz"
        weird_file.write_text("stuff")

        source = _joint("src_data", type="source", catalog="fs", table="data.xyz")
        query = _joint("q", upstream=["src_data"], sql="SELECT * FROM src_data")
        compiled: dict[str, Any] = {"src_data": source, "q": query}
        catalog_map: dict[str, Any] = {"fs": _fs_catalog(str(tmp_path))}

        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT * FROM src_data",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["src_data", "q"],
        )

        assert result is None


class TestNoCatalogPathIgnored:
    def test_no_catalog_path_ignored(self) -> None:
        """Catalog without 'path' → source ignored."""
        source = _joint("src_data", type="source", catalog="fs", table="data.csv")
        query = _joint("q", upstream=["src_data"], sql="SELECT * FROM src_data")
        compiled: dict[str, Any] = {"src_data": source, "q": query}
        # Catalog with no path
        cat = SimpleNamespace(type="filesystem", options={"type": "filesystem"})
        catalog_map: dict[str, Any] = {"fs": cat}

        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT * FROM src_data",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["src_data", "q"],
        )

        assert result is None


class TestMultipleSourcesAllResolved:
    def test_multiple_sources_all_resolved(self, tmp_path: Path) -> None:
        """Multiple sources resolved in the same SQL."""
        csv_file = tmp_path / "products.csv"
        csv_file.write_text("id,name\n1,Widget\n")
        pq_file = tmp_path / "orders.parquet"
        pq_file.write_bytes(b"PAR1")

        src_products = _joint("src_products", type="source", catalog="fs", table="products.csv")
        src_orders = _joint("src_orders", type="source", catalog="fs", table="orders.parquet")
        query = _joint(
            "q",
            upstream=["src_products", "src_orders"],
            sql="SELECT * FROM src_products JOIN src_orders ON src_products.id = src_orders.product_id",
        )
        compiled: dict[str, Any] = {
            "src_products": src_products,
            "src_orders": src_orders,
            "q": query,
        }
        catalog_map: dict[str, Any] = {"fs": _fs_catalog(str(tmp_path))}

        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT * FROM src_products JOIN src_orders ON src_products.id = src_orders.product_id",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["src_products", "src_orders", "q"],
        )

        assert result is not None
        assert f"read_csv_auto('{csv_file}')" in result
        assert f"read_parquet('{pq_file}')" in result


class TestPartialResolution:
    def test_partial_resolution(self, tmp_path: Path) -> None:
        """One source resolved, one ignored → partially resolved SQL."""
        csv_file = tmp_path / "products.csv"
        csv_file.write_text("id,name\n1,Widget\n")
        # src_missing has no file → ignored

        src_products = _joint("src_products", type="source", catalog="fs", table="products.csv")
        src_missing = _joint("src_missing", type="source", catalog="fs", table="nonexistent.csv")
        query = _joint(
            "q",
            upstream=["src_products", "src_missing"],
            sql="SELECT * FROM src_products JOIN src_missing ON src_products.id = src_missing.id",
        )
        compiled: dict[str, Any] = {
            "src_products": src_products,
            "src_missing": src_missing,
            "q": query,
        }
        catalog_map: dict[str, Any] = {"fs": _fs_catalog(str(tmp_path))}

        resolver = DuckDBReferenceResolver()
        result = resolver.resolve_references(
            sql="SELECT * FROM src_products JOIN src_missing ON src_products.id = src_missing.id",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["src_products", "src_missing", "q"],
        )

        assert result is not None
        # src_products resolved
        assert f"read_csv_auto('{csv_file}')" in result
        # src_missing still present (not resolved)
        assert "src_missing" in result
