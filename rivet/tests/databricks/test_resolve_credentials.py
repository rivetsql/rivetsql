"""Tests for _resolve_credentials helper in DatabricksUnityAdapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from rivet_core.errors import ExecutionError
from rivet_databricks.adapters.unity import _resolve_credentials


@dataclass
class FakeEngine:
    config: dict[str, Any] = field(default_factory=dict)


class TestResolveCredentials:
    """Unit tests for _resolve_credentials."""

    def test_returns_all_three_fields(self):
        engine = FakeEngine(config={
            "workspace_url": "https://adb-123.azuredatabricks.net",
            "token": "dapi_abc",
            "warehouse_id": "wh-001",
        })
        url, token, wh = _resolve_credentials(engine)
        assert url == "https://adb-123.azuredatabricks.net"
        assert token == "dapi_abc"
        assert wh == "wh-001"

    @pytest.mark.parametrize("missing_field", ["workspace_url", "token", "warehouse_id"])
    def test_missing_field_raises_rvt501(self, missing_field: str):
        config = {
            "workspace_url": "https://adb-123.azuredatabricks.net",
            "token": "dapi_abc",
            "warehouse_id": "wh-001",
        }
        del config[missing_field]
        engine = FakeEngine(config=config)
        with pytest.raises(ExecutionError) as exc_info:
            _resolve_credentials(engine)
        err = exc_info.value.error
        assert err.code == "RVT-501"
        assert missing_field in err.message
        assert err.context["plugin_name"] == "rivet_databricks"
        assert err.context["plugin_type"] == "adapter"
        assert err.context["adapter"] == "DatabricksUnityAdapter"
        assert missing_field in err.remediation

    @pytest.mark.parametrize("missing_field", ["workspace_url", "token", "warehouse_id"])
    def test_empty_string_field_raises_rvt501(self, missing_field: str):
        config = {
            "workspace_url": "https://adb-123.azuredatabricks.net",
            "token": "dapi_abc",
            "warehouse_id": "wh-001",
        }
        config[missing_field] = ""
        engine = FakeEngine(config=config)
        with pytest.raises(ExecutionError) as exc_info:
            _resolve_credentials(engine)
        assert exc_info.value.error.code == "RVT-501"

    def test_empty_config_raises_rvt501(self):
        engine = FakeEngine(config={})
        with pytest.raises(ExecutionError) as exc_info:
            _resolve_credentials(engine)
        assert exc_info.value.error.code == "RVT-501"

    def test_extra_config_fields_ignored(self):
        engine = FakeEngine(config={
            "workspace_url": "https://adb-123.azuredatabricks.net",
            "token": "dapi_abc",
            "warehouse_id": "wh-001",
            "wait_timeout": "30s",
            "extra_field": "ignored",
        })
        url, token, wh = _resolve_credentials(engine)
        assert url == "https://adb-123.azuredatabricks.net"
        assert token == "dapi_abc"
        assert wh == "wh-001"
