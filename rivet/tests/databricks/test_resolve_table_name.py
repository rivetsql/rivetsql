"""Tests for _resolve_table_name helper in DatabricksUnityAdapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from rivet_core.errors import ExecutionError
from rivet_databricks.adapters.unity import _resolve_table_name


@dataclass
class FakeJoint:
    name: str = "my_table"
    table: str | None = None


@dataclass
class FakeCatalog:
    name: str = "my_catalog"
    options: dict[str, Any] = field(default_factory=lambda: {
        "catalog_name": "prod",
        "schema": "analytics",
    })


class TestResolveTableName:
    """Unit tests for _resolve_table_name."""

    def test_uses_joint_table_when_set(self):
        joint = FakeJoint(table="cat.sch.tbl")
        catalog = FakeCatalog()
        assert _resolve_table_name(joint, catalog) == "cat.sch.tbl"

    def test_falls_back_to_default_table_reference(self):
        joint = FakeJoint(name="orders")
        catalog = FakeCatalog(options={"catalog_name": "prod", "schema": "sales"})
        assert _resolve_table_name(joint, catalog) == "prod.sales.orders"

    def test_default_schema_is_default(self):
        joint = FakeJoint(name="users")
        catalog = FakeCatalog(options={"catalog_name": "main"})
        assert _resolve_table_name(joint, catalog) == "main.default.users"

    def test_joint_table_overrides_catalog_options(self):
        joint = FakeJoint(name="ignored", table="other.schema.tbl")
        catalog = FakeCatalog(options={"catalog_name": "prod", "schema": "analytics"})
        assert _resolve_table_name(joint, catalog) == "other.schema.tbl"

    @pytest.mark.parametrize("bad_name", [
        "no_dots",
        "one.dot",
        "too.many.dots.here",
        "a.b.c.d.e",
    ])
    def test_invalid_table_name_raises_rvt503(self, bad_name: str):
        joint = FakeJoint(table=bad_name)
        catalog = FakeCatalog()
        with pytest.raises(ExecutionError) as exc_info:
            _resolve_table_name(joint, catalog)
        err = exc_info.value.error
        assert err.code == "RVT-503"
        assert bad_name in err.message
        assert err.context["plugin_name"] == "rivet_databricks"
        assert err.context["plugin_type"] == "adapter"
        assert err.context["adapter"] == "DatabricksUnityAdapter"
        assert "three-part" in err.remediation

    def test_empty_joint_table_falls_back(self):
        """Empty string for joint.table should fall back to default_table_reference."""
        joint = FakeJoint(name="events", table="")
        catalog = FakeCatalog(options={"catalog_name": "prod", "schema": "raw"})
        assert _resolve_table_name(joint, catalog) == "prod.raw.events"

    def test_none_joint_table_falls_back(self):
        joint = FakeJoint(name="events", table=None)
        catalog = FakeCatalog(options={"catalog_name": "prod", "schema": "raw"})
        assert _resolve_table_name(joint, catalog) == "prod.raw.events"
