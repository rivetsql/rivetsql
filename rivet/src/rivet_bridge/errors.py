"""Bridge error types and error code constants."""

from __future__ import annotations

from dataclasses import dataclass

# --- Error code constants ---

# Config / parse errors (1xx)
BRG_100_CONFIG_FAILURE = "BRG-100"
BRG_101_SQL_GEN_FAILURE = "BRG-101"
BRG_102_SQL_DECOMPOSITION_FAILURE = "BRG-102"

# Validation errors (2xx)
BRG_201_UNKNOWN_CATALOG_TYPE = "BRG-201"
BRG_202_CATALOG_VALIDATION = "BRG-202"
BRG_203_UNKNOWN_ENGINE_TYPE = "BRG-203"
BRG_204_ENGINE_VALIDATION = "BRG-204"
BRG_205_SINK_NO_UPSTREAM = "BRG-205"
BRG_206_AUDIT_NON_SINK = "BRG-206"
BRG_207_UNKNOWN_ENGINE_REF = "BRG-207"
BRG_208_DUPLICATE_JOINT = "BRG-208"
BRG_209_UNKNOWN_UPSTREAM = "BRG-209"
BRG_210_CYCLE = "BRG-210"
BRG_211_SOURCE_WITH_UPSTREAM = "BRG-211"

# Profile errors (3xx)
BRG_301_CREDENTIAL_TRACKING = "BRG-301"

# Roundtrip / output errors (4xx)
BRG_401_OUTPUT_DIR_CONFLICT = "BRG-401"
BRG_402_ROUNDTRIP_DIFFERENCE = "BRG-402"


@dataclass(frozen=True)
class BridgeError:
    """A single bridge-layer error with actionable context."""

    code: str
    message: str
    joint_name: str | None = None
    source_file: str | None = None
    remediation: str | None = None


class BridgeValidationError(Exception):
    """Raised when build_assembly encounters errors."""

    def __init__(self, errors: list[BridgeError]) -> None:
        self.errors = errors
        super().__init__(f"{len(errors)} bridge error(s)")
