"""Tests for AWS API error → RVT error code mapping (Task 10.1).

Validates Requirements 11.1–11.7:
- S3 NoSuchBucket → RVT-510
- S3 AccessDenied → RVT-511
- Glue EntityNotFoundException → RVT-512
- Glue AccessDeniedException → RVT-513
- All PluginValidationError payloads include plugin_name, plugin_type, code, remediation
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from rivet_aws.errors import handle_glue_error, handle_s3_error
from rivet_core.errors import ExecutionError

# ── Unit tests for handle_s3_error ────────────────────────────────────


def _make_client_error(code: str, message: str = "test", operation: str = "GetObject") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, operation)


class TestHandleS3Error:
    def test_no_such_bucket_returns_rvt_510(self):
        exc = _make_client_error("NoSuchBucket")
        result = handle_s3_error(exc, bucket="my-bucket")
        assert isinstance(result, ExecutionError)
        assert result.error.code == "RVT-510"
        assert "my-bucket" in result.error.message
        assert result.error.context["plugin_name"] == "rivet_aws"
        assert result.error.context["bucket"] == "my-bucket"
        assert result.error.remediation is not None

    def test_access_denied_returns_rvt_511(self):
        exc = _make_client_error("AccessDenied")
        result = handle_s3_error(exc, bucket="my-bucket", action="s3:ListBucket")
        assert isinstance(result, ExecutionError)
        assert result.error.code == "RVT-511"
        assert result.error.context["plugin_name"] == "rivet_aws"
        assert result.error.context["required_action"] == "s3:ListBucket"
        assert result.error.remediation is not None

    def test_403_returns_rvt_511(self):
        exc = _make_client_error("403")
        result = handle_s3_error(exc, bucket="my-bucket")
        assert result.error.code == "RVT-511"

    def test_unknown_error_returns_rvt_510_fallback(self):
        exc = _make_client_error("InternalError")
        result = handle_s3_error(exc, bucket="my-bucket")
        assert isinstance(result, ExecutionError)
        assert result.error.code == "RVT-510"


# ── Unit tests for handle_glue_error ──────────────────────────────────


class TestHandleGlueError:
    def test_entity_not_found_returns_rvt_512(self):
        exc = _make_client_error("EntityNotFoundException")
        result = handle_glue_error(exc, database="mydb", table="mytable")
        assert isinstance(result, ExecutionError)
        assert result.error.code == "RVT-512"
        assert "mytable" in result.error.message
        assert "mydb" in result.error.message
        assert result.error.context["plugin_name"] == "rivet_aws"
        assert result.error.context["database"] == "mydb"
        assert result.error.context["table"] == "mytable"
        assert result.error.remediation is not None

    def test_entity_not_found_database_only(self):
        exc = _make_client_error("EntityNotFoundException")
        result = handle_glue_error(exc, database="mydb")
        assert result.error.code == "RVT-512"
        assert "mydb" in result.error.message

    def test_access_denied_returns_rvt_513(self):
        exc = _make_client_error("AccessDeniedException")
        result = handle_glue_error(exc, database="mydb", action="glue:GetTable")
        assert isinstance(result, ExecutionError)
        assert result.error.code == "RVT-513"
        assert result.error.context["plugin_name"] == "rivet_aws"
        assert result.error.context["required_action"] == "glue:GetTable"
        assert result.error.remediation is not None

    def test_unknown_error_returns_rvt_512_fallback(self):
        exc = _make_client_error("InternalServiceException")
        result = handle_glue_error(exc, database="mydb")
        assert result.error.code == "RVT-512"


# ── Integration tests: S3 catalog error mapping ──────────────────────


class TestS3CatalogErrorMapping:
    @patch("rivet_aws.s3_catalog._build_s3fs")
    def test_get_metadata_no_such_bucket(self, mock_build_s3fs):
        """head_object with NoSuchBucket raises RVT-510."""
        from rivet_aws.s3_catalog import S3CatalogPlugin
        from rivet_core.models import Catalog

        mock_build_s3fs.return_value = MagicMock()

        plugin = S3CatalogPlugin()
        catalog = Catalog(name="test", type="s3", options={
            "bucket": "nonexistent-bucket",
            "format": "parquet",
            "region": "us-east-1",
        })

        mock_client = MagicMock()
        mock_client.head_object.side_effect = _make_client_error("NoSuchBucket")

        mock_resolver = MagicMock()
        mock_resolver.create_client.return_value = mock_client

        def mock_factory(options, region):
            return mock_resolver

        catalog.options["_credential_resolver_factory"] = mock_factory

        with pytest.raises(ExecutionError) as exc_info:
            plugin.get_metadata(catalog, "some_table")
        assert exc_info.value.error.code == "RVT-510"
        assert exc_info.value.error.context["bucket"] == "nonexistent-bucket"

    @patch("rivet_aws.s3_catalog._build_s3fs")
    def test_get_metadata_access_denied(self, mock_build_s3fs):
        """head_object with AccessDenied raises RVT-511."""
        from rivet_aws.s3_catalog import S3CatalogPlugin
        from rivet_core.models import Catalog

        mock_build_s3fs.return_value = MagicMock()

        plugin = S3CatalogPlugin()
        catalog = Catalog(name="test", type="s3", options={
            "bucket": "private-bucket",
            "format": "parquet",
            "region": "us-east-1",
        })

        mock_client = MagicMock()
        mock_client.head_object.side_effect = _make_client_error("AccessDenied")

        mock_resolver = MagicMock()
        mock_resolver.create_client.return_value = mock_client

        def mock_factory(options, region):
            return mock_resolver

        catalog.options["_credential_resolver_factory"] = mock_factory

        with pytest.raises(ExecutionError) as exc_info:
            plugin.get_metadata(catalog, "some_table")
        assert exc_info.value.error.code == "RVT-511"
        assert exc_info.value.error.context["required_action"] == "s3:GetObject"


# ── Integration tests: Glue catalog error mapping ────────────────────


class TestGlueCatalogErrorMapping:
    def _make_catalog(self):
        from rivet_core.models import Catalog
        return Catalog(name="test_glue", type="glue", options={
            "database": "mydb",
            "region": "us-east-1",
        })

    @patch("rivet_aws.glue_catalog._make_glue_client")
    def test_list_tables_entity_not_found(self, mock_make_client):
        from rivet_aws.glue_catalog import GlueCatalogPlugin

        mock_client = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.side_effect = _make_client_error("EntityNotFoundException")
        mock_client.get_paginator.return_value = mock_paginator
        mock_make_client.return_value = mock_client

        plugin = GlueCatalogPlugin()
        catalog = self._make_catalog()

        with pytest.raises(ExecutionError) as exc_info:
            plugin.list_tables(catalog)
        assert exc_info.value.error.code == "RVT-512"

    @patch("rivet_aws.glue_catalog._make_glue_client")
    def test_list_tables_access_denied(self, mock_make_client):
        from rivet_aws.glue_catalog import GlueCatalogPlugin

        mock_client = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.side_effect = _make_client_error("AccessDeniedException")
        mock_client.get_paginator.return_value = mock_paginator
        mock_make_client.return_value = mock_client

        plugin = GlueCatalogPlugin()
        catalog = self._make_catalog()

        with pytest.raises(ExecutionError) as exc_info:
            plugin.list_tables(catalog)
        assert exc_info.value.error.code == "RVT-513"

    @patch("rivet_aws.glue_catalog._make_glue_client_for_table")
    def test_get_schema_entity_not_found(self, mock_make_client):
        from rivet_aws.glue_catalog import GlueCatalogPlugin

        mock_client = MagicMock()
        mock_client.get_table.side_effect = _make_client_error("EntityNotFoundException")
        mock_make_client.return_value = mock_client

        plugin = GlueCatalogPlugin()
        catalog = self._make_catalog()

        with pytest.raises(ExecutionError) as exc_info:
            plugin.get_schema(catalog, "missing_table")
        assert exc_info.value.error.code == "RVT-512"
        assert exc_info.value.error.context["table"] == "missing_table"

    @patch("rivet_aws.glue_catalog._make_glue_client_for_table")
    def test_get_schema_access_denied(self, mock_make_client):
        from rivet_aws.glue_catalog import GlueCatalogPlugin

        mock_client = MagicMock()
        mock_client.get_table.side_effect = _make_client_error("AccessDeniedException")
        mock_make_client.return_value = mock_client

        plugin = GlueCatalogPlugin()
        catalog = self._make_catalog()

        with pytest.raises(ExecutionError) as exc_info:
            plugin.get_schema(catalog, "secret_table")
        assert exc_info.value.error.code == "RVT-513"

    @patch("rivet_aws.glue_catalog._make_glue_client_for_table")
    def test_get_metadata_entity_not_found(self, mock_make_client):
        from rivet_aws.glue_catalog import GlueCatalogPlugin

        mock_client = MagicMock()
        mock_client.get_table.side_effect = _make_client_error("EntityNotFoundException")
        mock_make_client.return_value = mock_client

        plugin = GlueCatalogPlugin()
        catalog = self._make_catalog()

        with pytest.raises(ExecutionError) as exc_info:
            plugin.get_metadata(catalog, "missing_table")
        assert exc_info.value.error.code == "RVT-512"


# ── Integration tests: Glue source error mapping ─────────────────────


class TestGlueSourceErrorMapping:
    @patch("rivet_aws.glue_catalog._make_glue_client")
    def test_resolve_glue_table_entity_not_found(self, mock_make_client):
        from rivet_aws.glue_source import _resolve_glue_table
        from rivet_core.models import Catalog

        mock_client = MagicMock()
        mock_client.get_table.side_effect = _make_client_error("EntityNotFoundException")
        mock_make_client.return_value = mock_client

        catalog = Catalog(name="test", type="glue", options={
            "database": "mydb",
            "region": "us-east-1",
        })

        with pytest.raises(ExecutionError) as exc_info:
            _resolve_glue_table(catalog, "missing_table", None)
        assert exc_info.value.error.code == "RVT-512"

    @patch("rivet_aws.glue_catalog._make_glue_client")
    def test_resolve_glue_table_access_denied(self, mock_make_client):
        from rivet_aws.glue_source import _resolve_glue_table
        from rivet_core.models import Catalog

        mock_client = MagicMock()
        mock_client.get_table.side_effect = _make_client_error("AccessDeniedException")
        mock_make_client.return_value = mock_client

        catalog = Catalog(name="test", type="glue", options={
            "database": "mydb",
            "region": "us-east-1",
        })

        with pytest.raises(ExecutionError) as exc_info:
            _resolve_glue_table(catalog, "secret_table", None)
        assert exc_info.value.error.code == "RVT-513"


# ── Integration tests: Glue sink error mapping ───────────────────────


class TestGlueSinkErrorMapping:
    @patch("rivet_aws.glue_catalog._make_glue_client")
    def test_table_exists_access_denied_raises(self, mock_make_client):
        from rivet_aws.glue_sink import _table_exists
        from rivet_core.models import Catalog

        mock_client = MagicMock()
        mock_client.get_table.side_effect = _make_client_error("AccessDeniedException")
        mock_make_client.return_value = mock_client

        catalog = Catalog(name="test", type="glue", options={
            "database": "mydb",
            "region": "us-east-1",
        })

        with pytest.raises(ExecutionError) as exc_info:
            _table_exists(mock_client, catalog, "secret_table")
        assert exc_info.value.error.code == "RVT-513"

    @patch("rivet_aws.glue_catalog._make_glue_client")
    def test_table_exists_entity_not_found_returns_false(self, mock_make_client):
        from rivet_aws.glue_sink import _table_exists
        from rivet_core.models import Catalog

        mock_client = MagicMock()
        mock_client.get_table.side_effect = _make_client_error("EntityNotFoundException")
        mock_make_client.return_value = mock_client

        catalog = Catalog(name="test", type="glue", options={
            "database": "mydb",
            "region": "us-east-1",
        })

        assert _table_exists(mock_client, catalog, "missing_table") is False


# ── Verify all PluginValidationError payloads have required fields ───


class TestPluginValidationErrorPayloads:
    """Verify that all PluginValidationError payloads include plugin_name, plugin_type, code, remediation."""

    def test_s3_catalog_missing_bucket(self):
        from rivet_aws.s3_catalog import S3CatalogPlugin
        from rivet_core.errors import PluginValidationError

        plugin = S3CatalogPlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({})
        err = exc_info.value.error
        assert err.context["plugin_name"] == "rivet_aws"
        assert err.context["plugin_type"] == "catalog"
        assert err.code.startswith("RVT-")
        assert err.remediation

    def test_s3_catalog_invalid_format(self):
        from rivet_aws.s3_catalog import S3CatalogPlugin
        from rivet_core.errors import PluginValidationError

        plugin = S3CatalogPlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"bucket": "test", "format": "xml"})
        err = exc_info.value.error
        assert err.context["plugin_name"] == "rivet_aws"
        assert err.code == "RVT-201"
        assert err.remediation

    def test_glue_catalog_invalid_auth_type(self):
        from rivet_aws.glue_catalog import GlueCatalogPlugin
        from rivet_core.errors import PluginValidationError

        plugin = GlueCatalogPlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"auth_type": "bad"})
        err = exc_info.value.error
        assert err.context["plugin_name"] == "rivet_aws"
        assert err.context["plugin_type"] == "catalog"
        assert err.code == "RVT-201"
        assert err.remediation

    def test_glue_sink_merge_rejected(self):
        from rivet_aws.glue_sink import _validate_sink_options
        from rivet_core.errors import PluginValidationError

        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "write_strategy": "merge"})
        err = exc_info.value.error
        assert err.context["plugin_name"] == "rivet_aws"
        assert err.code == "RVT-202"
        assert err.remediation

    def test_s3_sink_merge_without_delta(self):
        from rivet_aws.s3_sink import _parse_sink_options
        from rivet_core.errors import PluginValidationError

        mock_joint = MagicMock()
        mock_joint.table = "test"
        with pytest.raises(PluginValidationError) as exc_info:
            _parse_sink_options(
                {"bucket": "b", "sink_options": {"path": "p", "write_strategy": "merge", "format": "parquet"}},
                mock_joint,
            )
        err = exc_info.value.error
        assert err.context["plugin_name"] == "rivet_aws"
        assert err.code == "RVT-202"
        assert err.remediation

    def test_credential_no_source_resolved(self):
        """Verify credential resolution failure includes required payload fields."""
        from rivet_aws.credentials import AWSCredentialResolver
        from rivet_core.errors import PluginValidationError

        resolver = AWSCredentialResolver({}, "us-east-1")
        # Patch all resolution steps to return None
        for step in ["_try_explicit_options", "_try_aws_profile", "_try_environment_variables",
                      "_try_web_identity_token", "_try_ecs_task_role", "_try_ec2_imdsv2"]:
            setattr(resolver, step, lambda: None)

        with pytest.raises(PluginValidationError) as exc_info:
            resolver.resolve()
        err = exc_info.value.error
        assert err.context["plugin_name"] == "rivet_aws"
        assert err.code == "RVT-201"
        assert err.remediation
