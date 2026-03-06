"""Tests for task 12.2: PostgresDuckDBAdapter declares all 6 capabilities."""

from __future__ import annotations

from rivet_postgres.adapters.duckdb import PostgresDuckDBAdapter

ALL_6 = [
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
    "cast_pushdown",
    "join",
    "aggregation",
]


def test_all_6_capabilities_declared():
    adapter = PostgresDuckDBAdapter()
    assert sorted(adapter.capabilities) == sorted(ALL_6)


def test_cast_pushdown_included():
    adapter = PostgresDuckDBAdapter()
    assert "cast_pushdown" in adapter.capabilities
