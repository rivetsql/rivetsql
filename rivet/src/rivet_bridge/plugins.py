"""Optional plugin registration helpers for rivet-bridge.

Centralizes plugin discovery so rivet_cli does not need to import
plugin packages directly (module boundary enforcement).
"""

from __future__ import annotations

import logging
import sys
from importlib.metadata import entry_points

from rivet_core import PluginRegistry

log = logging.getLogger("rivet.bridge.plugins")


def register_optional_plugins(
    registry: PluginRegistry,
    only: frozenset[str] | None = None,
) -> None:
    """Register optional plugins via entry-point discovery.

    Uses ``importlib.metadata`` entry points (``rivet.plugins`` group)
    so that any installed plugin package is found automatically — no
    hardcoded import list needed.  Missing optional plugins are
    silently skipped (logged at DEBUG level).

    Parameters
    ----------
    registry:
        The plugin registry to populate.
    only:
        If provided, only load entry points whose name is in this set.
        This avoids importing heavy packages (e.g. pyspark) when they
        are not needed by the current profile.
    """
    eps = sorted(entry_points(group="rivet.plugins"), key=lambda ep: ep.name)
    for ep in eps:
        if only is not None and ep.name not in only:
            log.debug("Skipping plugin '%s' (not in required set)", ep.name)
            continue
        try:
            plugin_fn = ep.load()
            plugin_fn(registry)
        except ImportError as exc:
            log.debug("Skipping optional plugin '%s': %s", ep.name, exc)
        except Exception as exc:  # noqa: BLE001
            print(
                f"warning: failed to register plugin {ep.name}: {exc}",
                file=sys.stderr,
            )
            log.warning(
                "Failed to register plugin '%s': %s", ep.name, exc, exc_info=True
            )
