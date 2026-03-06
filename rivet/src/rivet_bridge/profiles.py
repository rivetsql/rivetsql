"""Profile generation: ResolvedProfile → profiles.yaml content."""

from __future__ import annotations

from rivet_bridge.models import FileOutput
from rivet_config import ResolvedProfile


class ProfileGenerator:
    """Generates profiles.yaml from a ResolvedProfile snapshot."""

    def generate(
        self,
        profile: ResolvedProfile,
        credential_sources: dict[str, str] | None = None,
    ) -> FileOutput:
        lines: list[str] = []

        if not credential_sources:
            lines.append("# NOTE: Credential references should be restored manually (e.g. ${VAR_NAME})")

        # Wrap in profile name so rivet-config can re-parse
        lines.append(f"{profile.name}:")
        lines.append(f"  default_engine: {profile.default_engine}")

        # Catalogs as mapping (name → {type, ...options})
        if profile.catalogs:
            lines.append("")
            lines.append("  catalogs:")
            for name in sorted(profile.catalogs):
                cat = profile.catalogs[name]
                lines.append(f"    {cat.name}:")
                lines.append(f"      type: {cat.type}")
                if cat.options:
                    for k in sorted(cat.options):
                        val = _resolve_value(cat.options[k], f"catalogs.{name}.{k}", credential_sources)
                        lines.append(f"      {k}: {val}")

        # Engines as list
        if profile.engines:
            lines.append("")
            lines.append("  engines:")
            for eng in sorted(profile.engines, key=lambda e: e.name):
                lines.append(f"    - name: {eng.name}")
                lines.append(f"      type: {eng.type}")
                if eng.catalogs:
                    lines.append(f"      catalogs: [{', '.join(eng.catalogs)}]")
                if eng.options:
                    for k in sorted(eng.options):
                        val = _resolve_value(eng.options[k], f"engines.{eng.name}.{k}", credential_sources)
                        lines.append(f"      {k}: {val}")

        return FileOutput(
            relative_path="profiles.yaml",
            content="\n".join(lines) + "\n",
            joint_name=None,
        )


def _resolve_value(
    value: object,
    key_path: str,
    credential_sources: dict[str, str] | None,
) -> object:
    """Return ${VAR_NAME} placeholder if credential_sources tracks this key, else the value."""
    if credential_sources and key_path in credential_sources:
        return f"${{{credential_sources[key_path]}}}"
    return value
