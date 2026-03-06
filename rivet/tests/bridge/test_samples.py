"""Sample project integration tests for the bridge forward and roundtrip paths.

Tests forward path against sample projects 01-minimal through 04-full-featured,
verifies Assembly structure, full roundtrip equivalence, and cross-format roundtrip.

Requirements: 15.1, 15.2, 15.3, 15.4
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from rivet_bridge.forward import build_assembly
from rivet_bridge.models import BridgeResult
from rivet_bridge.reverse import generate_project
from rivet_config import load_config
from rivet_core import PluginRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _write_yaml(path: Path, data: object) -> None:
    _write(path, yaml.dump(data, default_flow_style=False))


def _registry() -> PluginRegistry:
    r = PluginRegistry()
    r.register_builtins()
    return r


def _forward(root: Path) -> BridgeResult:
    """Parse config and run forward path, returning BridgeResult."""
    config = load_config(root)
    assert config.success, config.errors
    return build_assembly(config, _registry())


# ---------------------------------------------------------------------------
# Sample fixtures — engine names avoid conflict with builtin "arrow" instance
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_01(tmp_path: Path) -> Path:
    """01-minimal: source → sql → sink."""
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
            "default_engine": "eng1",
            "catalogs": {"main": {"type": "arrow"}},
            "engines": [{"name": "eng1", "type": "arrow", "catalogs": ["arrow"]}],
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


@pytest.fixture()
def sample_02(tmp_path: Path) -> Path:
    """02-multi-source with assertions."""
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
            "default_engine": "eng1",
            "catalogs": {"warehouse": {"type": "arrow"}},
            "engines": [{"name": "eng1", "type": "arrow", "catalogs": ["arrow"]}],
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
    (root / "quality").mkdir()
    _write_yaml(root / "quality" / "raw_products.yaml", [
        {"type": "not_null", "columns": ["id", "name"]},
        {"type": "row_count", "min": 1},
    ])
    return root


@pytest.fixture()
def sample_03(tmp_path: Path) -> Path:
    """03-multi-engine."""
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
            "default_engine": "eng1",
            "catalogs": {
                "lake": {"type": "arrow"},
                "warehouse": {"type": "arrow"},
            },
            "engines": [
                {"name": "eng1", "type": "arrow", "catalogs": ["arrow"]},
                {"name": "eng2", "type": "arrow", "catalogs": ["arrow"]},
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
        "-- rivet:engine: eng2\n"
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


@pytest.fixture()
def sample_04(tmp_path: Path) -> Path:
    """04-full-featured: multiple sources, SQL/YAML joints, tags, descriptions, quality."""
    root = tmp_path / "04-full-featured"
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
            "default_engine": "eng1",
            "catalogs": {
                "main": {"type": "arrow"},
                "pg": {"type": "arrow"},
            },
            "engines": [
                {"name": "eng1", "type": "arrow", "catalogs": ["arrow"]},
            ],
        }
    })
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
    (root / "joints").mkdir()
    _write(root / "joints" / "active_users.sql", (
        "-- rivet:name: active_users\n"
        "-- rivet:upstream: [raw_users, raw_events]\n"
        "-- rivet:tags: [analytics]\n"
        "-- rivet:description: Users with recent activity\n"
        "-- rivet:assert: not_null(id)\n"
        "SELECT u.id, u.email\n"
        "FROM raw_users u\n"
        "JOIN raw_events e ON u.id = e.user_id\n"
        "WHERE e.created_at > CURRENT_DATE - INTERVAL '30 days'\n"
    ))
    _write_yaml(root / "joints" / "user_summary.yaml", {
        "name": "user_summary",
        "type": "sql",
        "sql": "SELECT id, COUNT(*) AS event_count FROM raw_events GROUP BY id",
        "upstream": ["raw_events"],
    })
    (root / "sinks").mkdir()
    _write_yaml(root / "sinks" / "write_users.yaml", {
        "name": "write_users",
        "type": "sink",
        "catalog": "pg",
        "table": "dim_users",
        "upstream": ["active_users"],
        "write_strategy": {"mode": "merge", "key": "id"},
    })
    (root / "quality").mkdir()
    _write_yaml(root / "quality" / "raw_users.yaml", [
        {"type": "not_null", "columns": ["id", "email"]},
        {"type": "unique", "columns": ["id"]},
    ])
    return root


# ---------------------------------------------------------------------------
# Forward path tests
# ---------------------------------------------------------------------------


class TestSample01Forward:
    def test_assembly_structure(self, sample_01: Path) -> None:
        result = _forward(sample_01)
        assert set(result.assembly.joints.keys()) == {"raw_orders", "total_orders", "write_totals"}

    def test_joint_types(self, sample_01: Path) -> None:
        result = _forward(sample_01)
        assert result.assembly.joints["raw_orders"].joint_type == "source"
        assert result.assembly.joints["total_orders"].joint_type == "sql"
        assert result.assembly.joints["write_totals"].joint_type == "sink"

    def test_topological_order(self, sample_01: Path) -> None:
        result = _forward(sample_01)
        order = result.assembly.topological_order()
        assert order.index("raw_orders") < order.index("total_orders")
        assert order.index("total_orders") < order.index("write_totals")

    def test_catalogs_instantiated(self, sample_01: Path) -> None:
        result = _forward(sample_01)
        assert "main" in result.catalogs

    def test_engines_instantiated(self, sample_01: Path) -> None:
        result = _forward(sample_01)
        assert "eng1" in result.engines


class TestSample02Forward:
    def test_assembly_structure(self, sample_02: Path) -> None:
        result = _forward(sample_02)
        assert set(result.assembly.joints.keys()) == {
            "raw_products", "raw_categories", "enriched_products", "write_products",
        }

    def test_assertions_preserved(self, sample_02: Path) -> None:
        result = _forward(sample_02)
        enriched = result.assembly.joints["enriched_products"]
        assert len(enriched.assertions) == 2
        types = {a.type for a in enriched.assertions}
        assert types == {"not_null", "unique"}

    def test_upstream_references(self, sample_02: Path) -> None:
        result = _forward(sample_02)
        enriched = result.assembly.joints["enriched_products"]
        assert set(enriched.upstream) == {"raw_products", "raw_categories"}


class TestSample03Forward:
    def test_assembly_structure(self, sample_03: Path) -> None:
        result = _forward(sample_03)
        assert set(result.assembly.joints.keys()) == {"events", "agg_events", "write_agg"}

    def test_engine_override(self, sample_03: Path) -> None:
        result = _forward(sample_03)
        assert result.assembly.joints["agg_events"].engine == "eng2"

    def test_write_strategy(self, sample_03: Path) -> None:
        result = _forward(sample_03)
        assert result.assembly.joints["write_agg"].write_strategy == "replace"

    def test_multiple_engines(self, sample_03: Path) -> None:
        result = _forward(sample_03)
        assert len(result.engines) == 2


class TestSample04Forward:
    def test_assembly_structure(self, sample_04: Path) -> None:
        result = _forward(sample_04)
        assert set(result.assembly.joints.keys()) == {
            "raw_users", "raw_events", "active_users", "user_summary", "write_users",
        }

    def test_tags_and_description(self, sample_04: Path) -> None:
        result = _forward(sample_04)
        raw_users = result.assembly.joints["raw_users"]
        assert raw_users.tags == ["pii", "raw"]
        assert raw_users.description == "Raw user data"

    def test_write_strategy_merge(self, sample_04: Path) -> None:
        result = _forward(sample_04)
        assert result.assembly.joints["write_users"].write_strategy == "merge"

    def test_quality_checks_on_active_users(self, sample_04: Path) -> None:
        result = _forward(sample_04)
        active = result.assembly.joints["active_users"]
        assert len(active.assertions) >= 1
        assert any(a.type == "not_null" for a in active.assertions)


# ---------------------------------------------------------------------------
# Full roundtrip: forward → reverse → forward
# ---------------------------------------------------------------------------


class TestFullRoundtrip:
    def _roundtrip(self, root: Path, tmp_path: Path) -> None:
        """Run forward → reverse → forward and verify semantic equivalence."""
        result1 = _forward(root)

        out_dir = tmp_path / "roundtrip_output"
        generate_project(result1, format="yaml", output_dir=out_dir)

        result2 = _forward(out_dir)

        assert set(result1.assembly.joints.keys()) == set(result2.assembly.joints.keys())
        for name in result1.assembly.joints:
            j1 = result1.assembly.joints[name]
            j2 = result2.assembly.joints[name]
            assert j1.name == j2.name
            assert j1.joint_type == j2.joint_type
            assert set(j1.upstream) == set(j2.upstream)

    def test_sample_01_roundtrip(self, sample_01: Path, tmp_path: Path) -> None:
        self._roundtrip(sample_01, tmp_path)

    def test_sample_03_roundtrip(self, sample_03: Path, tmp_path: Path) -> None:
        self._roundtrip(sample_03, tmp_path)


# ---------------------------------------------------------------------------
# Cross-format roundtrip: YAML → SQL → re-import
# ---------------------------------------------------------------------------


class TestCrossFormatRoundtrip:
    def test_yaml_to_sql_roundtrip(self, sample_01: Path, tmp_path: Path) -> None:
        """Load YAML project, export as SQL, re-import, verify equivalence."""
        result1 = _forward(sample_01)

        sql_dir = tmp_path / "sql_output"
        generate_project(result1, format="sql", output_dir=sql_dir)

        result2 = _forward(sql_dir)

        assert set(result1.assembly.joints.keys()) == set(result2.assembly.joints.keys())
        for name in result1.assembly.joints:
            j1 = result1.assembly.joints[name]
            j2 = result2.assembly.joints[name]
            assert j1.name == j2.name
            assert j1.joint_type == j2.joint_type
            assert set(j1.upstream) == set(j2.upstream)

    def test_yaml_to_sql_roundtrip_multi_engine(self, sample_03: Path, tmp_path: Path) -> None:
        """Cross-format roundtrip preserves engine overrides."""
        result1 = _forward(sample_03)

        sql_dir = tmp_path / "sql_output"
        generate_project(result1, format="sql", output_dir=sql_dir)

        result2 = _forward(sql_dir)

        assert result2.assembly.joints["agg_events"].engine == "eng2"
        assert result2.assembly.joints["write_agg"].write_strategy == "replace"
