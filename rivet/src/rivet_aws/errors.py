"""AWS API error → RVT error code mapping for rivet_aws."""

from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError

from rivet_core.errors import ExecutionError, plugin_error


def _error_code(exc: ClientError) -> str:
    """Extract the AWS error code from a botocore ClientError."""
    return exc.response.get("Error", {}).get("Code", "")  # type: ignore[no-any-return]


def handle_s3_error(exc: ClientError, *, bucket: str, action: str = "s3:GetObject") -> ExecutionError:
    """Map an S3 ClientError to the appropriate RVT ExecutionError."""
    code = _error_code(exc)
    if code == "NoSuchBucket":
        return ExecutionError(
            plugin_error(
                "RVT-510",
                f"S3 bucket '{bucket}' does not exist.",
                plugin_name="rivet_aws",
                plugin_type="catalog",
                remediation=f"Verify the bucket name '{bucket}' is correct and exists in the expected region.",
                bucket=bucket,
            )
        )
    if code in ("AccessDenied", "403"):
        return ExecutionError(
            plugin_error(
                "RVT-511",
                f"Access denied to S3 bucket '{bucket}'.",
                plugin_name="rivet_aws",
                plugin_type="catalog",
                remediation=f"Ensure the IAM principal has '{action}' permission on bucket '{bucket}'.",
                bucket=bucket,
                required_action=action,
            )
        )
    # Fallback: re-wrap as generic execution error
    return ExecutionError(
        plugin_error(
            "RVT-510",
            f"S3 API error on bucket '{bucket}': {exc}",
            plugin_name="rivet_aws",
            plugin_type="catalog",
            remediation="Check S3 bucket configuration and IAM permissions.",
            bucket=bucket,
        )
    )


def handle_glue_error(
    exc: ClientError,
    *,
    database: str,
    table: str | None = None,
    action: str = "glue:GetTable",
) -> ExecutionError:
    """Map a Glue ClientError to the appropriate RVT ExecutionError."""
    code = _error_code(exc)
    if code == "EntityNotFoundException":
        entity = f"table '{table}' in database '{database}'" if table else f"database '{database}'"
        ctx: dict[str, Any] = {"database": database}
        if table:
            ctx["table"] = table
        return ExecutionError(
            plugin_error(
                "RVT-512",
                f"Glue {entity} not found.",
                plugin_name="rivet_aws",
                plugin_type="catalog",
                remediation=f"Verify that {entity} exists in the Glue Data Catalog.",
                **ctx,
            )
        )
    if code == "AccessDeniedException":
        return ExecutionError(
            plugin_error(
                "RVT-513",
                f"Access denied to Glue database '{database}'.",
                plugin_name="rivet_aws",
                plugin_type="catalog",
                remediation=f"Ensure the IAM principal has '{action}' permission on the Glue resource.",
                required_action=action,
            )
        )
    # Fallback
    return ExecutionError(
        plugin_error(
            "RVT-512",
            f"Glue API error: {exc}",
            plugin_name="rivet_aws",
            plugin_type="catalog",
            remediation="Check Glue Data Catalog configuration and IAM permissions.",
            database=database,
        )
    )
