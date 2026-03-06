"""Sample project integration tests for the compile command.

Exercises the full CLI compile pipeline (config → bridge → core → render)
against sample projects 01-minimal through 04-full-featured without mocking.

Requirements: 4.1, 4.10, 6.1, 7.1
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from rivet_cli.app import GlobalOptions
from rivet_cli.commands.compile import run_compile
from rivet_cli.exit_codes import SUCCESS
from rivet_core import PluginRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _write_yaml(path: Path, data: object) -> None:
    _write(path, yaml.dump(data, default_flow_style=False))


def _globals(project_path: Path) -> GlobalOptions:
    return GlobalOptions(profile="default", project_path=project_path, verbosity=0, color=False)


def _registry_with_builtins() -> PluginRegistry:
    r = PluginRegistry()
    r.register_builtins()
    return r


@pytest.fixture(autouse=True)
def _patch_registry():
    """Ensure PluginRegistry() in compile command has builtins registered."""
    with patch(
        "rivet_cli.commands.compile.PluginRegistry",
        side_effect=lambda: _registry_with_builtins(),
    ):
        yield


# ---------------------------------------------------------------------------
# Sample fixtures (use arrow engine/catalog — builtins registered via patch)
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
# Visual compile (default format) — successful compilation
# ---------------------------------------------------------------------------


class TestCompileVisual:
    def test_01_minimal(self, sample_01: Path, capsys) -> None:
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="visual", output=None, globals=_globals(sample_01),
        )
        assert result == SUCCESS
        out = capsys.readouterr().out
        assert "raw_orders" in out
        assert "total_orders" in out
        assert "write_totals" in out

    def test_02_multi_source_assertions(self, sample_02: Path, capsys) -> None:
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="visual", output=None, globals=_globals(sample_02),
        )
        assert result == SUCCESS
        out = capsys.readouterr().out
        assert "enriched_products" in out

    def test_03_multi_engine(self, sample_03: Path, capsys) -> None:
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="visual", output=None, globals=_globals(sample_03),
        )
        assert result == SUCCESS
        out = capsys.readouterr().out
        assert "agg_events" in out

    def test_04_full_featured(self, sample_04: Path, capsys) -> None:
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="visual", output=None, globals=_globals(sample_04),
        )
        assert result == SUCCESS
        out = capsys.readouterr().out
        assert "active_users" in out
        assert "user_summary" in out
        assert "write_users" in out


# ---------------------------------------------------------------------------
# JSON compile — valid JSON with expected structure
# ---------------------------------------------------------------------------


class TestCompileJSON:
    def _compile_json(self, project: Path, capsys) -> dict:
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="json", output=None, globals=_globals(project),
        )
        assert result == SUCCESS
        out = capsys.readouterr().out
        return json.loads(out)

    def test_01_minimal_json(self, sample_01: Path, capsys) -> None:
        data = self._compile_json(sample_01, capsys)
        assert "joints" in data
        assert "fused_groups" in data
        assert "execution_order" in data
        assert "catalogs" in data
        assert "engines" in data
        joint_names = {j["name"] for j in data["joints"]}
        assert joint_names == {"raw_orders", "total_orders", "write_totals"}

    def test_02_multi_source_assertions_json(self, sample_02: Path, capsys) -> None:
        data = self._compile_json(sample_02, capsys)
        enriched = next(j for j in data["joints"] if j["name"] == "enriched_products")
        assert len(enriched["checks"]) >= 2

    def test_03_multi_engine_json(self, sample_03: Path, capsys) -> None:
        data = self._compile_json(sample_03, capsys)
        assert len(data["engines"]) >= 2

    def test_04_full_featured_json(self, sample_04: Path, capsys) -> None:
        data = self._compile_json(sample_04, capsys)
        joint_names = {j["name"] for j in data["joints"]}
        assert joint_names == {"raw_users", "raw_events", "active_users", "user_summary", "write_users"}


# ---------------------------------------------------------------------------
# Mermaid compile — valid Mermaid graph
# ---------------------------------------------------------------------------


class TestCompileMermaid:
    def _compile_mermaid(self, project: Path, capsys) -> str:
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="mermaid", output=None, globals=_globals(project),
        )
        assert result == SUCCESS
        return capsys.readouterr().out

    def test_01_minimal_mermaid(self, sample_01: Path, capsys) -> None:
        out = self._compile_mermaid(sample_01, capsys)
        assert out.startswith("graph TD")
        assert "raw_orders" in out
        assert "total_orders" in out
        assert "-->" in out

    def test_02_multi_source_assertions_mermaid(self, sample_02: Path, capsys) -> None:
        out = self._compile_mermaid(sample_02, capsys)
        assert out.startswith("graph TD")
        assert "enriched_products" in out

    def test_03_multi_engine_mermaid(self, sample_03: Path, capsys) -> None:
        out = self._compile_mermaid(sample_03, capsys)
        assert out.startswith("graph TD")
        assert "subgraph" in out

    def test_04_full_featured_mermaid(self, sample_04: Path, capsys) -> None:
        out = self._compile_mermaid(sample_04, capsys)
        assert out.startswith("graph TD")
        assert "active_users" in out
        assert "write_users" in out
