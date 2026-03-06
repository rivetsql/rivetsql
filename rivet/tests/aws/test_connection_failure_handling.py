"""Tests for task 10.2: connection failure handling for Catalog Explorer.

Requirement 10.7: When S3 or Glue catalog fails to connect during startup,
include in list_catalogs() with connected=False and error message.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rivet_aws.glue_catalog import GlueCatalogPlugin
from rivet_aws.s3_catalog import S3CatalogPlugin
from rivet_core.catalog_explorer import CatalogExplorer
from rivet_core.models import Catalog
from rivet_core.plugins import PluginRegistry


def _make_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry.register_catalog_plugin(S3CatalogPlugin())
    registry.register_catalog_plugin(GlueCatalogPlugin())
    return registry


# ── S3 test_connection ────────────────────────────────────────────────────


def test_s3_test_connection_calls_head_bucket():
    """test_connection calls HeadBucket with the configured bucket."""
    plugin = S3CatalogPlugin()
    Catalog(
        name="my_s3",
        type="s3",
        options={"bucket": "test-bucket", "region": "us-east-1"},
    )
    mock_client = MagicMock()
    mock_resolver = MagicMock()
    mock_resolver.create_client.return_value = mock_client

    with patch("rivet_aws.s3_catalog._credential_resolver_factory", return_value=mock_resolver):
        catalog_with_factory = Catalog(
            name="my_s3",
            type="s3",
            options={
                "bucket": "test-bucket",
                "region": "us-east-1",
                "_credential_resolver_factory": lambda opts, region: mock_resolver,
            },
        )
        plugin.test_connection(catalog_with_factory)

    mock_client.head_bucket.assert_called_once_with(Bucket="test-bucket")


def test_s3_test_connection_raises_on_failure():
    """test_connection raises when HeadBucket fails."""
    plugin = S3CatalogPlugin()
    mock_client = MagicMock()
    mock_client.head_bucket.side_effect = Exception("NoSuchBucket")
    mock_resolver = MagicMock()
    mock_resolver.create_client.return_value = mock_client

    catalog = Catalog(
        name="my_s3",
        type="s3",
        options={
            "bucket": "missing-bucket",
            "region": "us-east-1",
            "_credential_resolver_factory": lambda opts, region: mock_resolver,
        },
    )
    with pytest.raises(Exception, match="NoSuchBucket"):
        plugin.test_connection(catalog)


# ── Glue test_connection ──────────────────────────────────────────────────


def test_glue_test_connection_calls_get_database():
    """test_connection calls GetDatabase with the configured database."""
    plugin = GlueCatalogPlugin()
    mock_client = MagicMock()

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=mock_client):
        catalog = Catalog(
            name="my_glue",
            type="glue",
            options={"database": "my_db", "region": "us-east-1"},
        )
        plugin.test_connection(catalog)

    mock_client.get_database.assert_called_once_with(Name="my_db")


def test_glue_test_connection_includes_catalog_id():
    """test_connection passes CatalogId when set."""
    plugin = GlueCatalogPlugin()
    mock_client = MagicMock()

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=mock_client):
        catalog = Catalog(
            name="my_glue",
            type="glue",
            options={"database": "my_db", "catalog_id": "123456789012"},
        )
        plugin.test_connection(catalog)

    mock_client.get_database.assert_called_once_with(Name="my_db", CatalogId="123456789012")


def test_glue_test_connection_raises_on_failure():
    """test_connection raises when GetDatabase fails."""
    plugin = GlueCatalogPlugin()
    mock_client = MagicMock()
    mock_client.get_database.side_effect = Exception("EntityNotFoundException")

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=mock_client):
        catalog = Catalog(
            name="my_glue",
            type="glue",
            options={"database": "missing_db"},
        )
        with pytest.raises(Exception, match="EntityNotFoundException"):
            plugin.test_connection(catalog)


# ── CatalogExplorer integration ───────────────────────────────────────────


def test_s3_connection_failure_appears_in_list_catalogs():
    """When S3 test_connection fails, list_catalogs() returns connected=False with error."""
    registry = _make_registry()
    mock_client = MagicMock()
    mock_client.head_bucket.side_effect = Exception("Connection refused")
    mock_resolver = MagicMock()
    mock_resolver.create_client.return_value = mock_client

    catalog = Catalog(
        name="broken_s3",
        type="s3",
        options={
            "bucket": "unreachable-bucket",
            "region": "us-east-1",
            "_credential_resolver_factory": lambda opts, region: mock_resolver,
        },
    )
    explorer = CatalogExplorer(
        catalogs={"broken_s3": catalog},
        engines={},
        registry=registry,
    )
    infos = explorer.list_catalogs()
    assert len(infos) == 1
    info = infos[0]
    assert info.name == "broken_s3"
    assert info.connected is False
    assert info.error is not None
    assert "Connection refused" in info.error


def test_glue_connection_failure_appears_in_list_catalogs():
    """When Glue test_connection fails, list_catalogs() returns connected=False with error."""
    registry = _make_registry()
    mock_client = MagicMock()
    mock_client.get_database.side_effect = Exception("Access denied to database")

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=mock_client):
        catalog = Catalog(
            name="broken_glue",
            type="glue",
            options={"database": "restricted_db", "region": "us-east-1"},
        )
        explorer = CatalogExplorer(
            catalogs={"broken_glue": catalog},
            engines={},
            registry=registry,
        )

    infos = explorer.list_catalogs()
    assert len(infos) == 1
    info = infos[0]
    assert info.name == "broken_glue"
    assert info.connected is False
    assert info.error is not None
    assert "Access denied" in info.error


def test_connected_catalog_not_affected_by_failing_catalog():
    """A failing catalog does not affect a connected catalog in list_catalogs()."""
    registry = _make_registry()

    # S3 catalog that fails
    mock_s3_client = MagicMock()
    mock_s3_client.head_bucket.side_effect = Exception("NoSuchBucket")
    mock_s3_resolver = MagicMock()
    mock_s3_resolver.create_client.return_value = mock_s3_client

    broken_s3 = Catalog(
        name="broken_s3",
        type="s3",
        options={
            "bucket": "missing",
            "region": "us-east-1",
            "_credential_resolver_factory": lambda opts, region: mock_s3_resolver,
        },
    )

    # Glue catalog that succeeds
    mock_glue_client = MagicMock()
    mock_glue_client.get_database.return_value = {"Database": {"Name": "good_db"}}

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=mock_glue_client):
        good_glue = Catalog(
            name="good_glue",
            type="glue",
            options={"database": "good_db", "region": "us-east-1"},
        )
        explorer = CatalogExplorer(
            catalogs={"broken_s3": broken_s3, "good_glue": good_glue},
            engines={},
            registry=registry,
        )

    infos = {i.name: i for i in explorer.list_catalogs()}
    assert infos["broken_s3"].connected is False
    assert infos["broken_s3"].error is not None
    assert infos["good_glue"].connected is True
    assert infos["good_glue"].error is None


def test_list_children_returns_empty_for_disconnected_catalog():
    """list_children() returns empty list for a catalog with connected=False."""
    registry = _make_registry()
    mock_client = MagicMock()
    mock_client.head_bucket.side_effect = Exception("Timeout")
    mock_resolver = MagicMock()
    mock_resolver.create_client.return_value = mock_client

    catalog = Catalog(
        name="broken_s3",
        type="s3",
        options={
            "bucket": "unreachable",
            "region": "us-east-1",
            "_credential_resolver_factory": lambda opts, region: mock_resolver,
        },
    )
    explorer = CatalogExplorer(
        catalogs={"broken_s3": catalog},
        engines={},
        registry=registry,
    )
    children = explorer.list_children(["broken_s3"])
    assert children == []
