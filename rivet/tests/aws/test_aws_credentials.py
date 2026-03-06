"""Tests for rivet_aws.credentials — 6-step AWS credential resolution chain."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from rivet_aws.credentials import RESOLUTION_STEPS, AWSCredentialResolver, AWSCredentials
from rivet_core.errors import PluginValidationError


class TestResolutionOrder:
    """Verify the 6-step resolution order is correct."""

    def test_resolution_steps_order(self):
        assert RESOLUTION_STEPS == [
            "explicit_options",
            "aws_profile",
            "environment_variables",
            "web_identity_token",
            "ecs_task_role",
            "ec2_imdsv2",
        ]


class TestExplicitOptions:
    """Step 1: explicit access_key_id + secret_access_key."""

    def test_resolves_from_explicit_options(self):
        resolver = AWSCredentialResolver(
            {"access_key_id": "AKID", "secret_access_key": "SECRET", "session_token": "TOK"},
            region="us-east-1",
        )
        creds = resolver.resolve()
        assert creds.access_key_id == "AKID"
        assert creds.secret_access_key == "SECRET"
        assert creds.session_token == "TOK"
        assert creds.source == "explicit_options"

    def test_explicit_without_session_token(self):
        resolver = AWSCredentialResolver(
            {"access_key_id": "AKID", "secret_access_key": "SECRET"},
            region="us-east-1",
        )
        creds = resolver.resolve()
        assert creds.session_token is None
        assert creds.source == "explicit_options"

    def test_explicit_takes_priority_over_env(self):
        """Explicit options should be tried before environment variables."""
        resolver = AWSCredentialResolver(
            {"access_key_id": "EXPLICIT_KEY", "secret_access_key": "EXPLICIT_SECRET"},
            region="us-east-1",
        )
        with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "ENV_KEY", "AWS_SECRET_ACCESS_KEY": "ENV_SECRET"}):
            creds = resolver.resolve()
        assert creds.access_key_id == "EXPLICIT_KEY"
        assert creds.source == "explicit_options"


class TestProfileResolution:
    """Step 2: AWS CLI profile."""

    @patch("rivet_aws.credentials.boto3.Session")
    def test_resolves_from_profile_option(self, mock_session_cls):
        frozen = MagicMock()
        frozen.access_key = "PROF_KEY"
        frozen.secret_key = "PROF_SECRET"
        frozen.token = "PROF_TOK"
        mock_creds = MagicMock()
        mock_creds.get_frozen_credentials.return_value = frozen
        mock_session = MagicMock()
        mock_session.get_credentials.return_value = mock_creds
        mock_session_cls.return_value = mock_session

        resolver = AWSCredentialResolver({"profile": "myprofile"}, region="us-west-2")
        creds = resolver.resolve()
        assert creds.access_key_id == "PROF_KEY"
        assert creds.source == "aws_profile"
        mock_session_cls.assert_called_with(profile_name="myprofile", region_name="us-west-2")

    @patch("rivet_aws.credentials.boto3.Session")
    def test_resolves_from_aws_profile_env(self, mock_session_cls):
        frozen = MagicMock()
        frozen.access_key = "PROF_KEY"
        frozen.secret_key = "PROF_SECRET"
        frozen.token = None
        mock_creds = MagicMock()
        mock_creds.get_frozen_credentials.return_value = frozen
        mock_session = MagicMock()
        mock_session.get_credentials.return_value = mock_creds
        mock_session_cls.return_value = mock_session

        with patch.dict(os.environ, {"AWS_PROFILE": "envprofile"}, clear=False):
            resolver = AWSCredentialResolver({}, region="us-east-1")
            creds = resolver.resolve()
        assert creds.source == "aws_profile"

    @patch("rivet_aws.credentials.boto3.Session")
    def test_profile_failure_falls_through(self, mock_session_cls):
        mock_session_cls.side_effect = Exception("profile not found")
        # Also clear env vars so env step doesn't resolve
        env_clean = {
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_PROFILE": "",
            "AWS_WEB_IDENTITY_TOKEN_FILE": "",
            "AWS_ROLE_ARN": "",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI": "",
        }
        with patch.dict(os.environ, env_clean, clear=False):
            # Patch IMDSv2 to also fail
            with patch("rivet_aws.credentials.InstanceMetadataFetcher") as mock_imds:
                mock_imds.return_value.retrieve_iam_role_credentials.side_effect = Exception("no imds")
                resolver = AWSCredentialResolver({"profile": "bad"}, region="us-east-1")
                with pytest.raises(PluginValidationError) as exc_info:
                    resolver.resolve()
                assert exc_info.value.error.code == "RVT-201"


class TestEnvironmentVariables:
    """Step 3: environment variables."""

    def test_resolves_from_env_vars(self):
        env = {
            "AWS_ACCESS_KEY_ID": "ENV_KEY",
            "AWS_SECRET_ACCESS_KEY": "ENV_SECRET",
            "AWS_SESSION_TOKEN": "ENV_TOK",
            "AWS_PROFILE": "",
        }
        with patch.dict(os.environ, env, clear=False):
            resolver = AWSCredentialResolver({}, region="eu-west-1")
            creds = resolver.resolve()
        assert creds.access_key_id == "ENV_KEY"
        assert creds.secret_access_key == "ENV_SECRET"
        assert creds.session_token == "ENV_TOK"
        assert creds.source == "environment_variables"


class TestWebIdentityToken:
    """Step 4: IRSA / web identity token file."""

    @patch("rivet_aws.credentials.boto3.client")
    def test_resolves_from_web_identity(self, mock_client_fn, tmp_path):
        token_file = tmp_path / "token"
        token_file.write_text("my-web-token")

        expiry_dt = datetime(2026, 1, 1, tzinfo=UTC)
        mock_sts = MagicMock()
        mock_sts.assume_role_with_web_identity.return_value = {
            "Credentials": {
                "AccessKeyId": "WI_KEY",
                "SecretAccessKey": "WI_SECRET",
                "SessionToken": "WI_TOK",
                "Expiration": expiry_dt,
            }
        }
        mock_client_fn.return_value = mock_sts

        env = {
            "AWS_WEB_IDENTITY_TOKEN_FILE": str(token_file),
            "AWS_ROLE_ARN": "arn:aws:iam::123456789012:role/my-role",
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_PROFILE": "",
        }
        with patch.dict(os.environ, env, clear=False):
            resolver = AWSCredentialResolver({}, region="us-east-1")
            creds = resolver.resolve()
        assert creds.access_key_id == "WI_KEY"
        assert creds.source == "web_identity_token"
        assert creds.expiry == expiry_dt.timestamp()


class TestECSTaskRole:
    """Step 5: ECS task role."""

    @patch("rivet_aws.credentials.ContainerMetadataFetcher")
    def test_resolves_from_ecs(self, mock_fetcher_cls):
        mock_fetcher = MagicMock()
        mock_fetcher.retrieve_uri.return_value = {
            "AccessKeyId": "ECS_KEY",
            "SecretAccessKey": "ECS_SECRET",
            "Token": "ECS_TOK",
            "Expiration": "2026-01-01T00:00:00Z",
        }
        mock_fetcher_cls.return_value = mock_fetcher

        env = {
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "/creds",
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_PROFILE": "",
            "AWS_WEB_IDENTITY_TOKEN_FILE": "",
        }
        with patch.dict(os.environ, env, clear=False):
            resolver = AWSCredentialResolver({}, region="us-east-1")
            creds = resolver.resolve()
        assert creds.access_key_id == "ECS_KEY"
        assert creds.source == "ecs_task_role"


class TestEC2IMDSv2:
    """Step 6: EC2 instance metadata (IMDSv2)."""

    @patch("rivet_aws.credentials.InstanceMetadataFetcher")
    def test_resolves_from_imdsv2(self, mock_fetcher_cls):
        mock_fetcher = MagicMock()
        mock_fetcher.retrieve_iam_role_credentials.return_value = {
            "access_key": "IMDS_KEY",
            "secret_key": "IMDS_SECRET",
            "token": "IMDS_TOK",
            "expiry_time": "2026-01-01T00:00:00Z",
        }
        mock_fetcher_cls.return_value = mock_fetcher

        env = {
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_PROFILE": "",
            "AWS_WEB_IDENTITY_TOKEN_FILE": "",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI": "",
        }
        with patch.dict(os.environ, env, clear=False):
            resolver = AWSCredentialResolver({}, region="us-east-1")
            creds = resolver.resolve()
        assert creds.access_key_id == "IMDS_KEY"
        assert creds.source == "ec2_imdsv2"


class TestNoCredentials:
    """All 6 steps fail → RVT-201."""

    @patch("rivet_aws.credentials.InstanceMetadataFetcher")
    def test_raises_rvt201_when_nothing_resolves(self, mock_fetcher_cls):
        mock_fetcher_cls.return_value.retrieve_iam_role_credentials.side_effect = Exception("no imds")

        env = {
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_PROFILE": "",
            "AWS_WEB_IDENTITY_TOKEN_FILE": "",
            "AWS_ROLE_ARN": "",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI": "",
        }
        with patch.dict(os.environ, env, clear=False):
            resolver = AWSCredentialResolver({}, region="us-east-1")
            with pytest.raises(PluginValidationError) as exc_info:
                resolver.resolve()
            err = exc_info.value.error
            assert err.code == "RVT-201"
            assert "attempted_sources" in err.context
            assert len(err.context["attempted_sources"]) == 6
            assert err.remediation is not None


class TestSTSAssumeRole:
    """STS AssumeRole after base resolution."""

    def test_assume_role_after_explicit(self):
        expiry_dt = datetime(2026, 6, 1, tzinfo=UTC)
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "ASSUMED_KEY",
                "SecretAccessKey": "ASSUMED_SECRET",
                "SessionToken": "ASSUMED_TOK",
                "Expiration": expiry_dt,
            }
        }

        with patch("rivet_aws.credentials.boto3.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.client.return_value = mock_sts
            mock_session_cls.return_value = mock_session

            resolver = AWSCredentialResolver(
                {
                    "access_key_id": "BASE_KEY",
                    "secret_access_key": "BASE_SECRET",
                    "role_arn": "arn:aws:iam::123456789012:role/target",
                    "role_session_name": "test-session",
                },
                region="us-east-1",
            )
            creds = resolver.resolve()

        assert creds.access_key_id == "ASSUMED_KEY"
        assert creds.source == "explicit_options+assume_role"
        mock_sts.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/target",
            RoleSessionName="test-session",
        )


    def test_assume_role_failure_raises_rvt201(self):
        with patch("rivet_aws.credentials.boto3.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.client.return_value.assume_role.side_effect = Exception("Access denied")
            mock_session_cls.return_value = mock_session

            resolver = AWSCredentialResolver(
                {
                    "access_key_id": "BASE_KEY",
                    "secret_access_key": "BASE_SECRET",
                    "role_arn": "arn:aws:iam::123456789012:role/bad-role",
                },
                region="us-east-1",
            )
            with pytest.raises(PluginValidationError) as exc_info:
                resolver.resolve()
            err = exc_info.value.error
            assert err.code == "RVT-201"
            assert "role_arn" in err.context
            assert err.remediation is not None


class TestCredentialCaching:
    """Credential caching and refresh."""

    def test_caches_credentials(self):
        resolver = AWSCredentialResolver(
            {"access_key_id": "K", "secret_access_key": "S"},
            region="us-east-1",
        )
        c1 = resolver.resolve()
        c2 = resolver.resolve()
        assert c1 is c2

    def test_cache_disabled(self):
        resolver = AWSCredentialResolver(
            {"access_key_id": "K", "secret_access_key": "S", "credential_cache": False},
            region="us-east-1",
        )
        c1 = resolver.resolve()
        c2 = resolver.resolve()
        # Both resolve successfully but are separate objects
        assert c1 is not c2
        assert c1.access_key_id == c2.access_key_id

    def test_expired_credentials_re_resolve(self):
        import time

        resolver = AWSCredentialResolver(
            {"access_key_id": "K", "secret_access_key": "S"},
            region="us-east-1",
        )
        creds = resolver.resolve()
        # Force expiry
        creds.expiry = time.time() - 100  # already expired
        # Bypass frozen by setting directly on the cached ref
        resolver._cached = creds
        c2 = resolver.resolve()
        assert c2 is not creds  # re-resolved


class TestAWSCredentialsExpiry:
    """AWSCredentials.expired property."""

    def test_no_expiry_not_expired(self):
        c = AWSCredentials("K", "S")
        assert not c.expired

    def test_future_expiry_not_expired(self):
        import time
        c = AWSCredentials("K", "S", expiry=time.time() + 3600)
        assert not c.expired

    def test_past_expiry_is_expired(self):
        import time
        c = AWSCredentials("K", "S", expiry=time.time() - 100)
        assert c.expired


class TestCreateClient:
    """create_client and create_boto3_session use explicit region."""

    def test_create_boto3_session_uses_region(self):
        with patch("rivet_aws.credentials.boto3.Session") as mock_cls:
            mock_cls.return_value = MagicMock()
            resolver = AWSCredentialResolver(
                {"access_key_id": "K", "secret_access_key": "S", "credential_cache": False},
                region="ap-southeast-1",
            )
            resolver.create_boto3_session()
            mock_cls.assert_called_with(
                aws_access_key_id="K",
                aws_secret_access_key="S",
                aws_session_token=None,
                region_name="ap-southeast-1",
            )

    def test_create_client_uses_explicit_region(self):
        mock_client = MagicMock()
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client

        with patch("rivet_aws.credentials.boto3.Session") as mock_cls:
            mock_cls.return_value = mock_session
            resolver = AWSCredentialResolver(
                {"access_key_id": "K", "secret_access_key": "S", "credential_cache": False},
                region="eu-central-1",
            )
            result = resolver.create_client("s3")

        mock_session.client.assert_called_with("s3", region_name="eu-central-1")
        assert result is mock_client
