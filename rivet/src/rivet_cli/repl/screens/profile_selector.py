"""Profile selector overlay for the Rivet REPL.

Triggered by Ctrl+Shift+P. Lists all profiles from profiles.yaml and
switches the active profile on selection.

Requirements: 22.1, 22.2, 22.3
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import yaml

try:
    from textual.app import ComposeResult
    from textual.binding import Binding
    from textual.containers import Vertical
    from textual.screen import ModalScreen
    from textual.widgets import Label, ListItem, ListView

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False

if TYPE_CHECKING:
    from rivet_core.interactive.session import InteractiveSession


def _load_profile_names(project_path: Path) -> list[str]:
    """Load profile names from profiles.yaml (or profiles/ directory).

    Returns a sorted list of profile names. Falls back to empty list on error.
    """
    # Locate profiles file via rivet.yaml manifest
    manifest_path = project_path / "rivet.yaml"
    profiles_path: Path | None = None

    if manifest_path.is_file():
        try:
            raw = yaml.safe_load(manifest_path.read_text())
            if isinstance(raw, dict) and "profiles" in raw:
                candidate = project_path / raw["profiles"]
                if candidate.exists():
                    profiles_path = candidate
        except Exception:  # noqa: BLE001
            pass

    if profiles_path is None:
        # Fallback: look for profiles.yaml or profiles/ next to rivet.yaml
        for name in ("profiles.yaml", "profiles.yml", "profiles"):
            candidate = project_path / name
            if candidate.exists():
                profiles_path = candidate
                break

    if profiles_path is None:
        return []

    names: set[str] = set()

    if profiles_path.is_dir():
        for f in profiles_path.iterdir():
            if f.is_file() and f.suffix in (".yaml", ".yml"):
                names.add(f.stem)
    elif profiles_path.is_file():
        try:
            raw = yaml.safe_load(profiles_path.read_text())
            if isinstance(raw, dict):
                names.update(raw.keys())
        except Exception:  # noqa: BLE001
            pass

    # Also include global profiles from ~/.rivet/
    home = Path.home()
    for global_path in (home / ".rivet" / "profiles.yaml", home / ".rivet" / "profiles"):
        if global_path.is_file():
            try:
                raw = yaml.safe_load(global_path.read_text())
                if isinstance(raw, dict):
                    names.update(raw.keys())
            except Exception:  # noqa: BLE001
                pass
        elif global_path.is_dir():
            for f in global_path.iterdir():
                if f.is_file() and f.suffix in (".yaml", ".yml"):
                    names.add(f.stem)

    return sorted(names)


if _TEXTUAL_AVAILABLE:

    class ProfileSelectorScreen(ModalScreen[str | None]):
        """Modal overlay listing all profiles. Returns selected profile name or None."""

        BINDINGS = [
            Binding("escape", "dismiss_none", "Cancel", show=True),
        ]

        DEFAULT_CSS = """
        ProfileSelectorScreen {
            align: center middle;
        }
        #profile-dialog {
            width: 50;
            height: auto;
            max-height: 20;
            border: thick $accent;
            background: $surface;
            padding: 1 2;
        }
        #profile-title {
            text-align: center;
            text-style: bold;
            margin-bottom: 1;
        }
        #profile-list {
            height: auto;
            max-height: 14;
        }
        .profile-item--active {
            text-style: bold;
            color: $accent;
        }
        """

        def __init__(
            self,
            session: InteractiveSession,
            project_path: Path,
        ) -> None:
            super().__init__()
            self._session = session
            self._project_path = project_path
            self._profiles = _load_profile_names(project_path)

        def compose(self) -> ComposeResult:
            with Vertical(id="profile-dialog"):
                yield Label("Select Profile", id="profile-title")
                items = []
                for name in self._profiles:
                    label = f"{'▶ ' if name == self._session.active_profile else '  '}{name}"
                    item = ListItem(Label(label), id=f"profile-{name}")
                    if name == self._session.active_profile:
                        item.add_class("profile-item--active")
                    items.append(item)
                if not items:
                    items.append(ListItem(Label("  (no profiles found)")))
                yield ListView(*items, id="profile-list")

        def on_mount(self) -> None:
            """Focus the list and scroll to the active profile."""
            lv = self.query_one("#profile-list", ListView)
            lv.focus()
            # Scroll to active profile
            for i, name in enumerate(self._profiles):
                if name == self._session.active_profile:
                    lv.index = i
                    break

        def on_list_view_selected(self, event: ListView.Selected) -> None:
            """Handle profile selection."""
            item_id = event.item.id or ""
            prefix = "profile-"
            if not item_id.startswith(prefix):
                self.dismiss(None)
                return
            profile_name = item_id[len(prefix):]
            self.dismiss(profile_name)

        def action_dismiss_none(self) -> None:
            self.dismiss(None)

else:  # pragma: no cover

    class ProfileSelectorScreen:  # type: ignore[no-redef]
        """Stub when Textual is not installed."""

        def __init__(self, session: object, project_path: Path) -> None:
            pass
