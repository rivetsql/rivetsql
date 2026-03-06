"""Tests for cross-joint adapter registration and lookup in PluginRegistry.

Covers task 2.1:
- register_cross_joint_adapter round-trip
- get_cross_joint_adapter lookup (hit and miss)
- Duplicate adapter registration raises PluginRegistrationError
"""

from __future__ import annotations

from typing import Any

import pytest

from rivet_core.plugins import (
    CrossJointAdapter,
    PluginRegistrationError,
    PluginRegistry,
)


class _FakeAdapter(CrossJointAdapter):
    def __init__(self, consumer: str, producer: str) -> None:
        self.consumer_engine_type = consumer
        self.producer_engine_type = producer

    def resolve_upstream(self, producer_ref: Any, consumer_engine: Any, joint_context: Any) -> Any:
        return None


class TestCrossJointAdapterRegistration:
    def test_register_and_lookup(self) -> None:
        reg = PluginRegistry()
        adapter = _FakeAdapter("databricks", "databricks")
        reg.register_cross_joint_adapter(adapter)
        assert reg.get_cross_joint_adapter("databricks", "databricks") is adapter

    def test_lookup_miss_returns_none(self) -> None:
        reg = PluginRegistry()
        assert reg.get_cross_joint_adapter("databricks", "duckdb") is None

    def test_multiple_adapters_different_keys(self) -> None:
        reg = PluginRegistry()
        a1 = _FakeAdapter("databricks", "databricks")
        a2 = _FakeAdapter("postgres", "postgres")
        reg.register_cross_joint_adapter(a1)
        reg.register_cross_joint_adapter(a2)
        assert reg.get_cross_joint_adapter("databricks", "databricks") is a1
        assert reg.get_cross_joint_adapter("postgres", "postgres") is a2

    def test_duplicate_raises(self) -> None:
        reg = PluginRegistry()
        reg.register_cross_joint_adapter(_FakeAdapter("databricks", "databricks"))
        with pytest.raises(PluginRegistrationError, match="already registered"):
            reg.register_cross_joint_adapter(_FakeAdapter("databricks", "databricks"))

    def test_duplicate_error_names_engine_types(self) -> None:
        reg = PluginRegistry()
        reg.register_cross_joint_adapter(_FakeAdapter("databricks", "duckdb"))
        with pytest.raises(PluginRegistrationError) as exc_info:
            reg.register_cross_joint_adapter(_FakeAdapter("databricks", "duckdb"))
        msg = str(exc_info.value)
        assert "databricks" in msg
        assert "duckdb" in msg

    def test_duplicate_error_names_existing_class(self) -> None:
        reg = PluginRegistry()
        reg.register_cross_joint_adapter(_FakeAdapter("databricks", "databricks"))
        with pytest.raises(PluginRegistrationError) as exc_info:
            reg.register_cross_joint_adapter(_FakeAdapter("databricks", "databricks"))
        assert "_FakeAdapter" in str(exc_info.value)

    def test_duplicate_error_includes_remediation(self) -> None:
        reg = PluginRegistry()
        reg.register_cross_joint_adapter(_FakeAdapter("databricks", "databricks"))
        with pytest.raises(PluginRegistrationError) as exc_info:
            reg.register_cross_joint_adapter(_FakeAdapter("databricks", "databricks"))
        msg = str(exc_info.value).lower()
        assert any(word in msg for word in ("uninstall", "duplicate", "conflict", "package"))

    def test_key_is_ordered_pair(self) -> None:
        """(consumer, producer) != (producer, consumer)."""
        reg = PluginRegistry()
        a1 = _FakeAdapter("databricks", "duckdb")
        a2 = _FakeAdapter("duckdb", "databricks")
        reg.register_cross_joint_adapter(a1)
        reg.register_cross_joint_adapter(a2)
        assert reg.get_cross_joint_adapter("databricks", "duckdb") is a1
        assert reg.get_cross_joint_adapter("duckdb", "databricks") is a2
