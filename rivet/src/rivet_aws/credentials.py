"""Shared AWS credential model for S3 and Glue catalogs.

6-step resolution chain:
  1. explicit options (access_key_id + secret_access_key)
  2. AWS CLI profile (~/.aws/credentials)
  3. environment variables (AWS_ACCESS_KEY_ID, etc.)
  4. web identity token file / IRSA (EKS)
  5. ECS task role (AWS_CONTAINER_CREDENTIALS_*)
  6. EC2 IMDSv2 (instance metadata)

Stops at the first successful source. If role_arn is set, calls STS AssumeRole
after base resolution. Caches credentials and refreshes before expiry.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

import boto3
import botocore.session
from botocore.credentials import (
    ContainerMetadataFetcher,
    InstanceMetadataFetcher,
)

from rivet_core.errors import PluginValidationError, plugin_error

RESOLUTION_STEPS = [
    "explicit_options",
    "aws_profile",
    "environment_variables",
    "web_identity_token",
    "ecs_task_role",
    "ec2_imdsv2",
]

_REFRESH_BUFFER_SECONDS = 60


@dataclass
class AWSCredentials:
    """Resolved AWS credentials."""

    access_key_id: str
    secret_access_key: str
    session_token: str | None = None
    expiry: float | None = None  # epoch seconds
    source: str = ""

    @property
    def expired(self) -> bool:
        if self.expiry is None:
            return False
        return time.time() >= (self.expiry - _REFRESH_BUFFER_SECONDS)


class AWSCredentialResolver:
    """Shared credential resolver for S3 and Glue catalogs."""

    def __init__(self, options: dict[str, Any], region: str) -> None:
        self._options = options
        self._region = region
        self._cached: AWSCredentials | None = None
        self._cache_enabled: bool = options.get("credential_cache", True)

    def resolve(self) -> AWSCredentials:
        """Resolve credentials through the 6-step chain, then optionally assume role."""
        if self._cache_enabled and self._cached is not None and not self._cached.expired:
            return self._cached

        creds = self._resolve_chain()

        role_arn = self._options.get("role_arn")
        if role_arn:
            creds = self._assume_role(creds, role_arn)

        if self._cache_enabled:
            self._cached = creds
        return creds

    def create_boto3_session(self) -> boto3.Session:
        """Create a boto3 Session with resolved credentials and explicit region."""
        creds = self.resolve()
        return boto3.Session(
            aws_access_key_id=creds.access_key_id,
            aws_secret_access_key=creds.secret_access_key,
            aws_session_token=creds.session_token,
            region_name=self._region,
        )

    def create_client(self, service: str) -> Any:
        """Create a boto3 client with resolved credentials and explicit region."""
        return self.create_boto3_session().client(service, region_name=self._region)

    # ── Resolution chain ──────────────────────────────────────────────

    def _resolve_chain(self) -> AWSCredentials:
        attempted: list[str] = []
        for step in RESOLUTION_STEPS:
            attempted.append(step)
            method = getattr(self, f"_try_{step}")
            result = method()
            if result is not None:
                return result  # type: ignore[no-any-return]

        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "No AWS credential source resolved.",
                plugin_name="rivet_aws",
                plugin_type="credential",
                remediation="Provide explicit access_key_id/secret_access_key, "
                    "set AWS_PROFILE or AWS_ACCESS_KEY_ID environment variables, "
                    "configure IRSA/ECS task role, or run on an EC2 instance with an instance profile.",
                attempted_sources=attempted,
            )
        )

    def _try_explicit_options(self) -> AWSCredentials | None:
        key = self._options.get("access_key_id")
        secret = self._options.get("secret_access_key")
        if key and secret:
            return AWSCredentials(
                access_key_id=key,
                secret_access_key=secret,
                session_token=self._options.get("session_token"),
                source="explicit_options",
            )
        return None

    def _try_aws_profile(self) -> AWSCredentials | None:
        profile = self._options.get("profile") or os.environ.get("AWS_PROFILE")
        if not profile:
            return None
        try:
            session = boto3.Session(profile_name=profile, region_name=self._region)
            creds = session.get_credentials()
            if creds is None:
                return None
            frozen = creds.get_frozen_credentials()
            return AWSCredentials(
                access_key_id=frozen.access_key,
                secret_access_key=frozen.secret_key,
                session_token=frozen.token,
                source="aws_profile",
            )
        except Exception:
            return None

    def _try_environment_variables(self) -> AWSCredentials | None:
        key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if key and secret:
            return AWSCredentials(
                access_key_id=key,
                secret_access_key=secret,
                session_token=os.environ.get("AWS_SESSION_TOKEN"),
                source="environment_variables",
            )
        return None

    def _try_web_identity_token(self) -> AWSCredentials | None:
        token_file = self._options.get("web_identity_token_file") or os.environ.get(
            "AWS_WEB_IDENTITY_TOKEN_FILE"
        )
        role_arn = os.environ.get("AWS_ROLE_ARN")
        if not token_file or not role_arn:
            return None
        try:
            with open(token_file) as f:
                token = f.read().strip()
            sts = boto3.client("sts", region_name=self._region)
            session_name = self._options.get("role_session_name", "rivet-session")
            resp = sts.assume_role_with_web_identity(
                RoleArn=role_arn,
                RoleSessionName=session_name,
                WebIdentityToken=token,
            )
            c = resp["Credentials"]
            return AWSCredentials(
                access_key_id=c["AccessKeyId"],
                secret_access_key=c["SecretAccessKey"],
                session_token=c["SessionToken"],
                expiry=c["Expiration"].timestamp(),
                source="web_identity_token",
            )
        except Exception:
            return None

    def _try_ecs_task_role(self) -> AWSCredentials | None:
        uri = os.environ.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") or os.environ.get(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI"
        )
        if not uri:
            return None
        try:
            botocore.session.Session()
            fetcher = ContainerMetadataFetcher()
            creds_dict = fetcher.retrieve_uri(uri)
            return AWSCredentials(
                access_key_id=creds_dict["AccessKeyId"],
                secret_access_key=creds_dict["SecretAccessKey"],
                session_token=creds_dict.get("Token"),
                expiry=_parse_expiry(creds_dict.get("Expiration")),
                source="ecs_task_role",
            )
        except Exception:
            return None

    def _try_ec2_imdsv2(self) -> AWSCredentials | None:
        try:
            fetcher = InstanceMetadataFetcher(timeout=1, num_attempts=1)
            creds = fetcher.retrieve_iam_role_credentials()
            return AWSCredentials(
                access_key_id=creds["access_key"],
                secret_access_key=creds["secret_key"],
                session_token=creds.get("token"),
                expiry=_parse_expiry(creds.get("expiry_time")),
                source="ec2_imdsv2",
            )
        except Exception:
            return None

    # ── STS AssumeRole ────────────────────────────────────────────────

    def _assume_role(self, base: AWSCredentials, role_arn: str) -> AWSCredentials:
        try:
            session = boto3.Session(
                aws_access_key_id=base.access_key_id,
                aws_secret_access_key=base.secret_access_key,
                aws_session_token=base.session_token,
                region_name=self._region,
            )
            sts = session.client("sts", region_name=self._region)
            session_name = self._options.get("role_session_name", "rivet-session")
            resp = sts.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
            c = resp["Credentials"]
            return AWSCredentials(
                access_key_id=c["AccessKeyId"],
                secret_access_key=c["SecretAccessKey"],
                session_token=c["SessionToken"],
                expiry=c["Expiration"].timestamp(),
                source=f"{base.source}+assume_role",
            )
        except PluginValidationError:
            raise
        except Exception as exc:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"STS AssumeRole failed for role '{role_arn}': {exc}",
                    plugin_name="rivet_aws",
                    plugin_type="credential",
                    remediation=f"Verify the role ARN '{role_arn}' exists, the trust policy allows "
                        "the base credentials to assume it, and the region is correct.",
                    role_arn=role_arn,
                )
            ) from exc


def _parse_expiry(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    # datetime object
    if hasattr(value, "timestamp"):
        return value.timestamp()  # type: ignore[no-any-return]
    # ISO string
    from datetime import datetime

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None
