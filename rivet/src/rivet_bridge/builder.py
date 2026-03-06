"""Assembly construction from converted joints."""

from __future__ import annotations

from rivet_bridge.errors import BridgeError
from rivet_bridge.models import BridgeResult
from rivet_config import ResolvedProfile
from rivet_core import Assembly, Catalog, ComputeEngine
from rivet_core.assembly import AssemblyError

_ERROR_CODE_MAP = {
    "RVT-301": "BRG-208",
    "RVT-302": "BRG-209",
    "RVT-303": "BRG-211",
    "RVT-304": "BRG-205",
    "RVT-305": "BRG-210",
}


class AssemblyBuilder:
    def build(
        self,
        joints: list,  # type: ignore[type-arg]
        catalogs: dict[str, Catalog],
        engines: dict[str, ComputeEngine],
        profile: ResolvedProfile,
        source_formats: dict[str, str],
    ) -> tuple[BridgeResult | None, list[BridgeError]]:
        errors: list[BridgeError] = []
        try:
            assembly = Assembly(joints)
        except AssemblyError as exc:
            code = _ERROR_CODE_MAP.get(exc.error.code, "BRG-208")
            errors.append(
                BridgeError(
                    code=code,
                    message=exc.error.message,
                    joint_name=exc.error.context.get("joint"),
                    remediation=exc.error.remediation,
                )
            )
            return None, errors

        result = BridgeResult(
            assembly=assembly,
            catalogs=catalogs,
            engines=engines,
            profile_snapshot=profile,
            source_formats=source_formats,
        )
        return result, errors
