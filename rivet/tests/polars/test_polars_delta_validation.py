"""Tests for task 30.4: Fail at validation time if deltalake not installed and Delta requested."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from rivet_core.errors import PluginValidationError
from rivet_polars.adapters.glue import GluePolarsAdapter
from rivet_polars.adapters.s3 import S3PolarsAdapter
from rivet_polars.adapters.unity import UnityPolarsAdapter


def _make_catalog(catalog_type: str, options: dict) -> object:
    from rivet_core.models import Catalog
    return Catalog(name="test", type=catalog_type, options=options)


# ── S3PolarsAdapter ───────────────────────────────────────────────────────────


class TestS3PolarsAdapterDeltaValidation:
    def test_delta_format_without_deltalake_raises_at_validation(self):
        adapter = S3PolarsAdapter()
        opts = {"bucket": "my-bucket", "format": "delta"}
        with patch.dict("sys.modules", {"deltalake": None}):
            with pytest.raises(PluginValidationError) as exc_info:
                adapter.validate_catalog_options(opts)
        assert exc_info.value.error.code == "RVT-201"
        assert "deltalake" in exc_info.value.error.message.lower() or "deltalake" in (exc_info.value.error.remediation or "").lower()

    def test_delta_format_with_deltalake_installed_does_not_raise(self):
        import importlib
        import types
        fake_deltalake = types.ModuleType("deltalake")
        fake_deltalake.__spec__ = importlib.machinery.ModuleSpec("deltalake", None)
        adapter = S3PolarsAdapter()
        opts = {"bucket": "my-bucket", "format": "delta"}
        with patch.dict("sys.modules", {"deltalake": fake_deltalake}):
            adapter.validate_catalog_options(opts)  # should not raise

    def test_non_delta_format_without_deltalake_does_not_raise(self):
        adapter = S3PolarsAdapter()
        opts = {"bucket": "my-bucket", "format": "parquet"}
        with patch.dict("sys.modules", {"deltalake": None}):
            adapter.validate_catalog_options(opts)  # should not raise

    def test_error_includes_install_suggestion(self):
        adapter = S3PolarsAdapter()
        opts = {"bucket": "my-bucket", "format": "delta"}
        with patch.dict("sys.modules", {"deltalake": None}):
            with pytest.raises(PluginValidationError) as exc_info:
                adapter.validate_catalog_options(opts)
        remediation = exc_info.value.error.remediation or ""
        assert "pip install" in remediation or "deltalake" in remediation


# ── GluePolarsAdapter ─────────────────────────────────────────────────────────


class TestGluePolarsAdapterDeltaValidation:
    def test_delta_format_without_deltalake_raises_at_validation(self):
        adapter = GluePolarsAdapter()
        catalog = _make_catalog("glue", {"database": "mydb", "format": "delta"})
        with patch.dict("sys.modules", {"deltalake": None}):
            with pytest.raises(PluginValidationError) as exc_info:
                adapter.validate(catalog)
        assert exc_info.value.error.code == "RVT-201"

    def test_delta_format_with_deltalake_installed_does_not_raise(self):
        import types
        fake_deltalake = types.ModuleType("deltalake")
        adapter = GluePolarsAdapter()
        catalog = _make_catalog("glue", {"database": "mydb", "format": "delta"})
        with patch.dict("sys.modules", {"deltalake": fake_deltalake}):
            adapter.validate(catalog)  # should not raise

    def test_non_delta_format_without_deltalake_does_not_raise(self):
        adapter = GluePolarsAdapter()
        catalog = _make_catalog("glue", {"database": "mydb", "format": "parquet"})
        with patch.dict("sys.modules", {"deltalake": None}):
            adapter.validate(catalog)  # should not raise


# ── UnityPolarsAdapter ────────────────────────────────────────────────────────


class TestUnityPolarsAdapterDeltaValidation:
    def test_delta_format_without_deltalake_raises_at_validation(self):
        adapter = UnityPolarsAdapter()
        catalog = _make_catalog("unity", {"host": "https://example.com", "catalog_name": "main", "format": "delta"})
        with patch.dict("sys.modules", {"deltalake": None}):
            with pytest.raises(PluginValidationError) as exc_info:
                adapter.validate(catalog)
        assert exc_info.value.error.code == "RVT-201"

    def test_delta_format_with_deltalake_installed_does_not_raise(self):
        import types
        fake_deltalake = types.ModuleType("deltalake")
        adapter = UnityPolarsAdapter()
        catalog = _make_catalog("unity", {"host": "https://example.com", "catalog_name": "main", "format": "delta"})
        with patch.dict("sys.modules", {"deltalake": fake_deltalake}):
            adapter.validate(catalog)  # should not raise

    def test_non_delta_format_without_deltalake_does_not_raise(self):
        adapter = UnityPolarsAdapter()
        catalog = _make_catalog("unity", {"host": "https://example.com", "catalog_name": "main", "format": "parquet"})
        with patch.dict("sys.modules", {"deltalake": None}):
            adapter.validate(catalog)  # should not raise
