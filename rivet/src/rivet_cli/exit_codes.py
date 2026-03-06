"""Exit code constants for the CLI."""

SUCCESS = 0
GENERAL_ERROR = 1
PARTIAL_FAILURE = 2
TEST_FAILURE = 3
ASSERTION_FAILURE = 4
AUDIT_FAILURE = 5
USAGE_ERROR = 10
INTERRUPTED = 130


def resolve_exit_code(
    has_assertion_failure: bool,
    has_audit_failure: bool,
    has_partial_failure: bool,
) -> int:
    """Resolve the most specific exit code when multiple failure types occur.

    Priority: ASSERTION_FAILURE > AUDIT_FAILURE > PARTIAL_FAILURE.
    """
    if has_assertion_failure:
        return ASSERTION_FAILURE
    if has_audit_failure:
        return AUDIT_FAILURE
    if has_partial_failure:
        return PARTIAL_FAILURE
    return SUCCESS
