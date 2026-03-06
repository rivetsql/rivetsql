"""Integration tests for sample projects parsed through load_config().

Validates end-to-end behavior: manifest parsing, profile resolution,
joint declaration loading, and quality check attachment.

Requirements: 1.1, 7.1, 8.1, 9.1, 11.1, 12.1, 14.1, 18.1
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from rivet_config import load_config

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _write_yaml(path: Path, data: object) -> None:
    _write(path, yaml.dump(data, default_flow_style=False))


# ---------------------------------------------------------------------------
# Sample 01 — minimal
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_01(tmp_path: Path) -> Path:
    root = tmp_path / "01-minimal"
    root.mkdir()
    _write_yaml(root / "rivet.yaml", {
        "profiles": "profiles.yaml",
        "sources": "sources",
        "joints": "joints",
        "sinks": "sinks",
    })
    _write_yaml(root / "profiles.yaml", {
        "default": {
            "default_engine": "duckdb",
            "catalogs": {"main": {"type": "duckdb"}},
            "engines": [{"name": "duckdb", "type": "duckdb", "catalogs": ["duckdb"]}],
        }
    })
    (root / "sources").mkdir()
    _write_yaml(root / "sources" / "raw_orders.yaml", {
        "name": "raw_orders",
        "type": "source",
        "catalog": "main",
        "columns": ["id", "customer_id", "amount"],
    })
    (root / "joints").mkdir()
    _write(root / "joints" / "total_orders.sql", (
        "-- rivet:name: total_orders\n"
        "-- rivet:upstream: [raw_orders]\n"
        "SELECT customer_id, SUM(amount) AS total\n"
        "FROM raw_orders\n"
    ))
    (root / "sinks").mkdir()
    _write_yaml(root / "sinks" / "write_totals.yaml", {
        "name": "write_totals",
        "type": "sink",
        "catalog": "main",
        "table": "totals",
        "upstream": ["total_orders"],
    })
    return root


class TestSample01Minimal:
    def test_success(self, sample_01: Path) -> None:
        result = load_config(sample_01)
        assert result.success, result.errors

    def test_manifest_paths(self, sample_01: Path) -> None:
        result = load_config(sample_01)
        m = result.manifest
        assert m is not None
        assert m.project_root == sample_01
        assert m.sources_dir == sample_01 / "sources"
        assert m.joints_dir == sample_01 / "joints"
        assert m.sinks_dir == sample_01 / "sinks"
        assert m.quality_dir is None

    def test_profile(self, sample_01: Path) -> None:
        result = load_config(sample_01)
        p = result.profile
        assert p is not None
        assert p.name == "default"
        assert p.default_engine == "duckdb"
        assert "main" in p.catalogs
        assert p.catalogs["main"].type == "duckdb"
        assert len(p.engines) == 1
        assert p.engines[0].name == "duckdb"

    def test_declarations(self, sample_01: Path) -> None:
        result = load_config(sample_01)
        names = [d.name for d in result.declarations]
        assert "raw_orders" in names
        assert "total_orders" in names
        assert "write_totals" in names

    def test_joint_types(self, sample_01: Path) -> None:
        result = load_config(sample_01)
        by_name = {d.name: d for d in result.declarations}
        assert by_name["raw_orders"].joint_type == "source"
        assert by_name["total_orders"].joint_type == "sql"
        assert by_name["write_totals"].joint_type == "sink"

    def test_sql_body(self, sample_01: Path) -> None:
        result = load_config(sample_01)
        sql_joint = next(d for d in result.declarations if d.name == "total_orders")
        assert sql_joint.sql is not None
        assert "SUM(amount)" in sql_joint.sql

    def test_sink_default_write_strategy(self, sample_01: Path) -> None:
        result = load_config(sample_01)
        sink = next(d for d in result.declarations if d.name == "write_totals")
        assert sink.write_strategy is not None
        assert sink.write_strategy.mode == "append"


# ---------------------------------------------------------------------------
# Sample 02 — multi-source with assertions
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_02(tmp_path: Path) -> Path:
    root = tmp_path / "02-multi-source-assertions"
    root.mkdir()
    _write_yaml(root / "rivet.yaml", {
        "profiles": "profiles.yaml",
        "sources": "sources",
        "joints": "joints",
        "sinks": "sinks",
        "quality": "quality",
    })
    _write_yaml(root / "profiles.yaml", {
        "default": {
            "default_engine": "duckdb",
            "catalogs": {"warehouse": {"type": "duckdb"}},
            "engines": [{"name": "duckdb", "type": "duckdb", "catalogs": ["duckdb"]}],
        }
    })
    (root / "sources").mkdir()
    _write_yaml(root / "sources" / "raw_products.yaml", {
        "name": "raw_products",
        "type": "source",
        "catalog": "warehouse",
        "columns": ["id", "name", "price"],
    })
    _write_yaml(root / "sources" / "raw_categories.yaml", {
        "name": "raw_categories",
        "type": "source",
        "catalog": "warehouse",
    })
    (root / "joints").mkdir()
    # SQL joint with inline assertion annotations
    _write(root / "joints" / "enriched_products.sql", (
        "-- rivet:name: enriched_products\n"
        "-- rivet:upstream: [raw_products, raw_categories]\n"
        "-- rivet:assert: not_null(id, name)\n"
        "-- rivet:assert: unique(id)\n"
        "SELECT p.*, c.name AS category\n"
        "FROM raw_products p\n"
        "JOIN raw_categories c ON p.category_id = c.id\n"
    ))
    (root / "sinks").mkdir()
    _write_yaml(root / "sinks" / "write_products.yaml", {
        "name": "write_products",
        "type": "sink",
        "catalog": "warehouse",
        "table": "products",
        "upstream": ["enriched_products"],
    })
    # Dedicated quality file targeting raw_products
    (root / "quality").mkdir()
    _write_yaml(root / "quality" / "raw_products.yaml", [
        {"type": "not_null", "columns": ["id", "name"]},
        {"type": "row_count", "min": 1},
    ])
    return root


class TestSample02MultiSourceAssertions:
    def test_success(self, sample_02: Path) -> None:
        result = load_config(sample_02)
        assert result.success, result.errors

    def test_all_joints_present(self, sample_02: Path) -> None:
        result = load_config(sample_02)
        names = {d.name for d in result.declarations}
        assert names == {"raw_products", "raw_categories", "enriched_products", "write_products"}

    def test_sql_annotation_assertions_attached(self, sample_02: Path) -> None:
        result = load_config(sample_02)
        enriched = next(d for d in result.declarations if d.name == "enriched_products")
        sql_checks = [c for c in enriched.quality_checks if c.source == "sql_annotation"]
        assert len(sql_checks) == 2
        types = {c.check_type for c in sql_checks}
        assert types == {"not_null", "unique"}
        assert all(c.phase == "assertion" for c in sql_checks)

    def test_dedicated_quality_attached_to_correct_joint(self, sample_02: Path) -> None:
        result = load_config(sample_02)
        raw_prod = next(d for d in result.declarations if d.name == "raw_products")
        dedicated = [c for c in raw_prod.quality_checks if c.source == "dedicated"]
        assert len(dedicated) == 2
        types = [c.check_type for c in dedicated]
        assert "not_null" in types
        assert "row_count" in types

    def test_unrelated_joint_has_no_quality(self, sample_02: Path) -> None:
        result = load_config(sample_02)
        raw_cat = next(d for d in result.declarations if d.name == "raw_categories")
        assert raw_cat.quality_checks == []


# ---------------------------------------------------------------------------
# Sample 03 — multi-engine
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_03(tmp_path: Path) -> Path:
    root = tmp_path / "03-multi-engine"
    root.mkdir()
    _write_yaml(root / "rivet.yaml", {
        "profiles": "profiles.yaml",
        "sources": "sources",
        "joints": "joints",
        "sinks": "sinks",
    })
    _write_yaml(root / "profiles.yaml", {
        "default": {
            "default_engine": "duckdb",
            "catalogs": {
                "lake": {"type": "duckdb"},
                "warehouse": {"type": "postgres", "host": "localhost"},
            },
            "engines": [
                {"name": "duckdb", "type": "duckdb", "catalogs": ["duckdb"]},
                {"name": "spark", "type": "spark", "catalogs": ["spark"]},
            ],
        }
    })
    (root / "sources").mkdir()
    _write_yaml(root / "sources" / "events.yaml", {
        "name": "events",
        "type": "source",
        "catalog": "lake",
    })
    (root / "joints").mkdir()
    _write(root / "joints" / "agg_events.sql", (
        "-- rivet:name: agg_events\n"
        "-- rivet:engine: spark\n"
        "-- rivet:upstream: [events]\n"
        "SELECT event_type, COUNT(*) AS cnt FROM events GROUP BY event_type\n"
    ))
    (root / "sinks").mkdir()
    _write_yaml(root / "sinks" / "write_agg.yaml", {
        "name": "write_agg",
        "type": "sink",
        "catalog": "warehouse",
        "table": "agg_events",
        "upstream": ["agg_events"],
        "write_strategy": {"mode": "replace"},
    })
    return root


class TestSample03MultiEngine:
    def test_success(self, sample_03: Path) -> None:
        result = load_config(sample_03)
        assert result.success, result.errors

    def test_multiple_catalogs(self, sample_03: Path) -> None:
        result = load_config(sample_03)
        p = result.profile
        assert p is not None
        assert len(p.catalogs) == 2
        assert "lake" in p.catalogs
        assert "warehouse" in p.catalogs

    def test_multiple_engines(self, sample_03: Path) -> None:
        result = load_config(sample_03)
        p = result.profile
        assert p is not None
        assert len(p.engines) == 2
        engine_names = {e.name for e in p.engines}
        assert engine_names == {"duckdb", "spark"}

    def test_engine_override_on_joint(self, sample_03: Path) -> None:
        result = load_config(sample_03)
        agg = next(d for d in result.declarations if d.name == "agg_events")
        assert agg.engine == "spark"

    def test_write_strategy_replace(self, sample_03: Path) -> None:
        result = load_config(sample_03)
        sink = next(d for d in result.declarations if d.name == "write_agg")
        assert sink.write_strategy is not None
        assert sink.write_strategy.mode == "replace"


# ---------------------------------------------------------------------------
# Sample 04 — full-featured
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_04(tmp_path: Path) -> Path:
    root = tmp_path / "04-full-featured"
    root.mkdir()
    _write_yaml(root / "rivet.yaml", {
        "profiles": "profiles",
        "sources": "sources",
        "joints": "joints",
        "sinks": "sinks",
        "quality": "quality",
    })
    # Directory-based profiles
    profiles_dir = root / "profiles"
    profiles_dir.mkdir()
    _write_yaml(profiles_dir / "default.yaml", {
        "default_engine": "duckdb",
        "catalogs": {
            "main": {"type": "duckdb"},
            "pg": {"type": "postgres", "host": "localhost"},
        },
        "engines": [
            {"name": "duckdb", "type": "duckdb", "catalogs": ["duckdb"]},
        ],
    })
    _write_yaml(profiles_dir / "production.yaml", {
        "default_engine": "duckdb",
        "catalogs": {
            "main": {"type": "duckdb"},
            "pg": {"type": "postgres", "host": "prod-db.example.com"},
        },
        "engines": [
            {"name": "duckdb", "type": "duckdb", "catalogs": ["duckdb"]},
        ],
    })

    # Sources
    (root / "sources").mkdir()
    _write_yaml(root / "sources" / "raw_users.yaml", {
        "name": "raw_users",
        "type": "source",
        "catalog": "main",
        "columns": ["id", "email", "created_at"],
        "description": "Raw user data",
        "tags": ["pii", "raw"],
    })
    _write_yaml(root / "sources" / "raw_events.yaml", {
        "name": "raw_events",
        "type": "source",
        "catalog": "main",
    })

    # Joints
    (root / "joints").mkdir()
    # SQL joint with annotations
    _write(root / "joints" / "active_users.sql", (
        "-- rivet:name: active_users\n"
        "-- rivet:upstream: [raw_users, raw_events]\n"
        "-- rivet:tags: [analytics]\n"
        "-- rivet:description: Users with recent activity\n"
        "-- rivet:assert: not_null(id)\n"
        "-- rivet:audit: row_count(min=1)\n"
        "SELECT u.id, u.email\n"
        "FROM raw_users u\n"
        "JOIN raw_events e ON u.id = e.user_id\n"
        "WHERE e.created_at > CURRENT_DATE - INTERVAL '30 days'\n"
    ))
    # YAML joint with inline quality
    _write_yaml(root / "joints" / "user_summary.yaml", {
        "name": "user_summary",
        "type": "sql",
        "sql": "SELECT id, COUNT(*) AS event_count FROM raw_events GROUP BY id",
        "upstream": ["raw_events"],
        "quality": {
            "assertions": [
                {"type": "not_null", "columns": ["id"]},
            ],
            "audits": [
                {"type": "row_count", "min": 0},
            ],
        },
    })

    # Sinks
    (root / "sinks").mkdir()
    _write_yaml(root / "sinks" / "write_users.yaml", {
        "name": "write_users",
        "type": "sink",
        "catalog": "pg",
        "table": "dim_users",
        "upstream": ["active_users"],
        "write_strategy": {"mode": "merge", "key": "id"},
        "columns": [
            "id",
            {"full_name": "first_name || ' ' || last_name"},
        ],
    })

    # Dedicated quality files
    (root / "quality").mkdir()
    _write_yaml(root / "quality" / "raw_users.yaml", {
        "assertions": [
            {"type": "not_null", "columns": ["id", "email"]},
            {"type": "unique", "columns": ["id"]},
        ],
    })

    # Co-located quality file alongside source — not a joint (no name+type),
    # so DeclarationLoader classifies it as co-located quality.
    # Filename stem "raw_events_quality" targets joint "raw_events_quality" — but we
    # want to target "raw_events". Use a YAML file with explicit joint: field instead,
    # or just use a filename that matches. We'll use a dedicated-style file with
    # assertions/audits sections and a stem matching the joint name.
    # However, the stem must match the joint name. Since "raw_events.yaml" is already
    # the joint file, we put a co-located quality file in a subdirectory.
    sub = root / "sources" / "checks"
    sub.mkdir()
    _write_yaml(sub / "raw_events.yaml", {
        "assertions": [
            {"type": "not_null", "columns": ["user_id"]},
        ],
    })

    return root


class TestSample04FullFeatured:
    def test_success(self, sample_04: Path) -> None:
        result = load_config(sample_04)
        assert result.success, result.errors

    def test_directory_based_profiles(self, sample_04: Path) -> None:
        result = load_config(sample_04)
        p = result.profile
        assert p is not None
        assert p.name == "default"

    def test_explicit_profile_selection(self, sample_04: Path) -> None:
        result = load_config(sample_04, profile_name="production")
        assert result.success, result.errors
        assert result.profile is not None
        assert result.profile.name == "production"
        # Production has different host
        assert result.profile.catalogs["pg"].options["host"] == "prod-db.example.com"

    def test_all_declarations_present(self, sample_04: Path) -> None:
        result = load_config(sample_04)
        names = {d.name for d in result.declarations}
        assert names == {"raw_users", "raw_events", "active_users", "user_summary", "write_users"}

    def test_sql_annotation_quality(self, sample_04: Path) -> None:
        result = load_config(sample_04)
        active = next(d for d in result.declarations if d.name == "active_users")
        sql_checks = [c for c in active.quality_checks if c.source == "sql_annotation"]
        assert len(sql_checks) == 2
        phases = {c.phase for c in sql_checks}
        assert phases == {"assertion", "audit"}

    def test_inline_yaml_quality(self, sample_04: Path) -> None:
        result = load_config(sample_04)
        summary = next(d for d in result.declarations if d.name == "user_summary")
        inline = [c for c in summary.quality_checks if c.source == "inline"]
        assert len(inline) == 2
        assert any(c.phase == "assertion" for c in inline)
        assert any(c.phase == "audit" for c in inline)

    def test_dedicated_quality_files(self, sample_04: Path) -> None:
        result = load_config(sample_04)
        raw_users = next(d for d in result.declarations if d.name == "raw_users")
        dedicated = [c for c in raw_users.quality_checks if c.source == "dedicated"]
        assert len(dedicated) == 2
        types = {c.check_type for c in dedicated}
        assert types == {"not_null", "unique"}

    def test_write_strategy_merge(self, sample_04: Path) -> None:
        result = load_config(sample_04)
        sink = next(d for d in result.declarations if d.name == "write_users")
        assert sink.write_strategy is not None
        assert sink.write_strategy.mode == "merge"
        assert sink.write_strategy.options.get("key") == "id"

    def test_column_expressions(self, sample_04: Path) -> None:
        result = load_config(sample_04)
        sink = next(d for d in result.declarations if d.name == "write_users")
        assert sink.columns is not None
        assert len(sink.columns) == 2
        assert sink.columns[0].name == "id"
        assert sink.columns[0].expression is None
        assert sink.columns[1].name == "full_name"
        assert sink.columns[1].expression is not None

    def test_tags_and_description(self, sample_04: Path) -> None:
        result = load_config(sample_04)
        raw_users = next(d for d in result.declarations if d.name == "raw_users")
        assert raw_users.tags == ["pii", "raw"]
        assert raw_users.description == "Raw user data"

    def test_declarations_sorted_by_source_file(self, sample_04: Path) -> None:
        result = load_config(sample_04)
        paths = [d.source_path for d in result.declarations]
        assert paths == sorted(paths)

    def test_colocated_quality_files(self, sample_04: Path) -> None:
        result = load_config(sample_04)
        raw_events = next(d for d in result.declarations if d.name == "raw_events")
        colocated = [c for c in raw_events.quality_checks if c.source == "colocated"]
        assert len(colocated) == 1
        assert colocated[0].check_type == "not_null"

    def test_quality_attachment_ordering(self, sample_04: Path) -> None:
        """Quality checks follow deterministic order: inline → sql_annotation → dedicated → colocated."""
        result = load_config(sample_04)
        for decl in result.declarations:
            sources = [c.source for c in decl.quality_checks]
            order = {"inline": 0, "sql_annotation": 1, "dedicated": 2, "colocated": 3}
            indices = [order[s] for s in sources]
            assert indices == sorted(indices), (
                f"Quality checks on '{decl.name}' not in expected order: {sources}"
            )
