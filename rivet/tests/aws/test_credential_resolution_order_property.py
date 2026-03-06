"""Property test for AWS credential resolution order.

Property 2: Credential resolution order
Generate random combinations of credential sources, mock each, verify
highest-priority wins.
Validates: Requirements 2.2
"""

from __future__ import annotations

import os
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_aws.credentials import RESOLUTION_STEPS, AWSCredentialResolver

# Unique access key per step so we can identify which source won.
_STEP_KEY = {
    "explicit_options": "EXPLICIT_KEY",
    "aws_profile": "PROFILE_KEY",
    "environment_variables": "ENV_KEY",
    "web_identity_token": "WI_KEY",
    "ecs_task_role": "ECS_KEY",
    "ec2_imdsv2": "IMDS_KEY",
}
_STEP_SECRET = {step: f"{step.upper()}_SECRET" for step in RESOLUTION_STEPS}


def _make_profile_session(key: str, secret: str) -> MagicMock:
    frozen = MagicMock()
    frozen.access_key = key
    frozen.secret_key = secret
    frozen.token = None
    mock_creds = MagicMock()
    mock_creds.get_frozen_credentials.return_value = frozen
    mock_session = MagicMock()
    mock_session.get_credentials.return_value = mock_creds
    return mock_session


def _run_with_available_steps(available: frozenset[int]) -> str:
    """Resolve credentials with exactly the given steps available; return winning access_key_id."""
    available_steps = {RESOLUTION_STEPS[i] for i in available}

    options: dict = {}
    env_vars: dict[str, str] = {
        "AWS_ACCESS_KEY_ID": "",
        "AWS_SECRET_ACCESS_KEY": "",
        "AWS_SESSION_TOKEN": "",
        "AWS_PROFILE": "",
        "AWS_WEB_IDENTITY_TOKEN_FILE": "",
        "AWS_ROLE_ARN": "",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI": "",
    }

    # Step 1: explicit_options
    if "explicit_options" in available_steps:
        options["access_key_id"] = _STEP_KEY["explicit_options"]
        options["secret_access_key"] = _STEP_SECRET["explicit_options"]

    # Step 2: aws_profile
    if "aws_profile" in available_steps:
        options["profile"] = "test-profile"

    # Step 3: environment_variables
    if "environment_variables" in available_steps:
        env_vars["AWS_ACCESS_KEY_ID"] = _STEP_KEY["environment_variables"]
        env_vars["AWS_SECRET_ACCESS_KEY"] = _STEP_SECRET["environment_variables"]

    # Step 4: web_identity_token
    if "web_identity_token" in available_steps:
        env_vars["AWS_WEB_IDENTITY_TOKEN_FILE"] = "/fake/token"
        env_vars["AWS_ROLE_ARN"] = "arn:aws:iam::123:role/r"

    # Step 5: ecs_task_role
    if "ecs_task_role" in available_steps:
        env_vars["AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"] = "/creds"

    with ExitStack() as stack:
        # Mock boto3.Session: used by aws_profile step and by create_boto3_session.
        # We need it to succeed for profile step when available, fail otherwise.
        if "aws_profile" in available_steps:
            profile_session = _make_profile_session(
                _STEP_KEY["aws_profile"], _STEP_SECRET["aws_profile"]
            )
            stack.enter_context(
                patch("rivet_aws.credentials.boto3.Session", return_value=profile_session)
            )
        else:
            stack.enter_context(
                patch("rivet_aws.credentials.boto3.Session", side_effect=Exception("no profile"))
            )

        # Mock boto3.client: used by web_identity_token step (STS assume_role_with_web_identity).
        if "web_identity_token" in available_steps:
            mock_sts = MagicMock()
            expiry_mock = MagicMock()
            expiry_mock.timestamp.return_value = 9999999999.0
            mock_sts.assume_role_with_web_identity.return_value = {
                "Credentials": {
                    "AccessKeyId": _STEP_KEY["web_identity_token"],
                    "SecretAccessKey": _STEP_SECRET["web_identity_token"],
                    "SessionToken": "WI_TOK",
                    "Expiration": expiry_mock,
                }
            }
            stack.enter_context(
                patch("rivet_aws.credentials.boto3.client", return_value=mock_sts)
            )
            # Mock open for token file read
            mock_file = MagicMock()
            mock_file.__enter__ = lambda s: MagicMock(read=lambda: "token")
            mock_file.__exit__ = MagicMock(return_value=False)
            stack.enter_context(patch("builtins.open", return_value=mock_file))

        # Mock ContainerMetadataFetcher: used by ecs_task_role step.
        if "ecs_task_role" in available_steps:
            mock_container = MagicMock()
            mock_container.retrieve_uri.return_value = {
                "AccessKeyId": _STEP_KEY["ecs_task_role"],
                "SecretAccessKey": _STEP_SECRET["ecs_task_role"],
                "Token": "ECS_TOK",
                "Expiration": None,
            }
            stack.enter_context(
                patch("rivet_aws.credentials.ContainerMetadataFetcher", return_value=mock_container)
            )
        else:
            stack.enter_context(
                patch(
                    "rivet_aws.credentials.ContainerMetadataFetcher",
                    side_effect=Exception("no ecs"),
                )
            )

        # Mock InstanceMetadataFetcher: used by ec2_imdsv2 step.
        if "ec2_imdsv2" in available_steps:
            mock_imds = MagicMock()
            mock_imds.retrieve_iam_role_credentials.return_value = {
                "access_key": _STEP_KEY["ec2_imdsv2"],
                "secret_key": _STEP_SECRET["ec2_imdsv2"],
                "token": "IMDS_TOK",
                "expiry_time": None,
            }
            stack.enter_context(
                patch("rivet_aws.credentials.InstanceMetadataFetcher", return_value=mock_imds)
            )
        else:
            stack.enter_context(
                patch(
                    "rivet_aws.credentials.InstanceMetadataFetcher",
                    side_effect=Exception("no imds"),
                )
            )

        with patch.dict(os.environ, env_vars, clear=False):
            resolver = AWSCredentialResolver(options, region="us-east-1")
            creds = resolver.resolve()
            return creds.access_key_id


@settings(max_examples=100, deadline=None)
@given(available_indices=st.frozensets(st.integers(min_value=0, max_value=5), min_size=1))
def test_property_highest_priority_source_wins(available_indices: frozenset[int]):
    """Property: when multiple credential sources are available, the one with the
    lowest step index (highest priority) always wins.

    Generates random subsets of the 6 resolution steps, mocks each available step
    to return a unique access key, and verifies the resolver returns the key from
    the highest-priority (lowest-index) available step.
    """
    expected_step = RESOLUTION_STEPS[min(available_indices)]
    expected_key = _STEP_KEY[expected_step]

    winning_key = _run_with_available_steps(available_indices)

    assert winning_key == expected_key, (
        f"Expected source '{expected_step}' (key={expected_key!r}) to win, "
        f"but got key='{winning_key}'. "
        f"Available step indices: {sorted(available_indices)}"
    )
