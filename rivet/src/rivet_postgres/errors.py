"""PostgreSQL connection error classification utilities.

Distinguishes authentication errors from connectivity errors when a PostgreSQL
connection attempt fails, enabling more actionable error messages.
"""

from __future__ import annotations

# Keywords in exception messages that indicate authentication failures.
# These come from PostgreSQL FATAL messages forwarded by psycopg3.
_AUTH_KEYWORDS = (
    "password authentication failed",
    "authentication failed",
    "role",  # "role \"user\" does not exist"
    "no pg_hba.conf entry",
    "peer authentication failed",
    "ident authentication failed",
    "gss authentication failed",
    "scram authentication failed",
    "md5 authentication failed",
    "invalid password",
    "password required",
)

# Keywords that indicate a network/connectivity failure.
_CONNECTIVITY_KEYWORDS = (
    "could not connect to server",
    "connection refused",
    "name or service not known",
    "no route to host",
    "network is unreachable",
    "connection timed out",
    "timeout expired",
    "server closed the connection",
    "ssl connection has been closed",
    "connection reset by peer",
    "broken pipe",
)


def is_auth_error(exc: BaseException) -> bool:
    """Return True if *exc* looks like a PostgreSQL authentication failure."""
    msg = str(exc).lower()
    return any(kw in msg for kw in _AUTH_KEYWORDS)


def is_connectivity_error(exc: BaseException) -> bool:
    """Return True if *exc* looks like a network/connectivity failure."""
    msg = str(exc).lower()
    return any(kw in msg for kw in _CONNECTIVITY_KEYWORDS)


def classify_pg_error(
    exc: BaseException,
    *,
    plugin_type: str,
    context: dict | None = None,  # type: ignore[type-arg]
) -> tuple[str, str, str]:
    """Classify a PostgreSQL connection/query exception.

    Returns a (code, message, remediation) triple suitable for use with
    ``plugin_error()``.

    - Authentication errors → RVT-502
    - Connectivity errors   → RVT-501
    - Other errors          → RVT-501
    """
    if is_auth_error(exc):
        return (
            "RVT-502",
            f"PostgreSQL authentication failed: {exc}",
            "Check the 'user' and 'password' options. Ensure the role exists and has login privileges.",
        )
    if is_connectivity_error(exc):
        return (
            "RVT-501",
            f"PostgreSQL connectivity error: {exc}",
            "Check that the PostgreSQL host is reachable, the port is correct, and the server is running.",
        )
    return (
        "RVT-501",
        f"PostgreSQL error: {exc}",
        "Check the SQL, table schema, and PostgreSQL server logs for details.",
    )
