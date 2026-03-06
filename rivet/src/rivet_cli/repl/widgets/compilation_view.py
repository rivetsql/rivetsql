"""CompilationView widget for the Rivet REPL TUI.

Renders compiled assembly output in the Compilation tab using the same
``render_visual`` renderer as ``rivet compile`` CLI — single source of truth.

Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

try:
    from textual.app import ComposeResult
    from textual.containers import VerticalScroll
    from textual.widget import Widget
    from textual.widgets import Static

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False

if TYPE_CHECKING:
    from rivet_core.compiler import CompiledAssembly


class _CompilationViewState:
    """Pure state for CompilationView — no TUI dependencies."""

    _session: Any
    _assembly: Any  # CompiledAssembly | None
    _errors: list[str]
    _placeholder: bool
    _verbosity: int

    def _init_state(self, session: Any | None = None) -> None:
        self._session = session
        self._assembly = None
        self._errors = []
        self._placeholder = True
        self._verbosity = 0

    def show_assembly(self, assembly: CompiledAssembly, verbosity: int = 0) -> None:
        """Store compiled assembly for rendering via render_visual."""
        self._assembly = assembly
        self._errors = []
        self._placeholder = False
        self._verbosity = verbosity
        self._on_state_changed()

    def show_errors(self, errors: list[str]) -> None:
        """Store compilation errors for rendering."""
        self._assembly = None
        self._errors = list(errors)
        self._placeholder = False
        self._on_state_changed()

    def show_placeholder(self) -> None:
        """Reset to placeholder state."""
        self._assembly = None
        self._errors = []
        self._placeholder = True
        self._on_state_changed()

    def _on_state_changed(self) -> None:
        """Hook for subclasses."""


if _TEXTUAL_AVAILABLE:

    class CompilationView(Widget, _CompilationViewState):
        """Compilation tab — renders assembly via render_visual as Static lines."""

        DEFAULT_CSS = """
        CompilationView {
            height: 1fr;
        }
        CompilationView .cv-placeholder {
            color: $text-muted;
            text-style: italic;
            padding: 1 2;
        }
        CompilationView .cv-row {
            padding: 0 1;
        }
        CompilationView .cv-error {
            color: red;
            padding: 0 1;
        }
        CompilationView .cv-footer {
            color: $text-muted;
            dock: bottom;
            height: 1;
            padding: 0 1;
        }
        """

        def __init__(self, session: Any | None = None, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self._init_state(session)

        def compose(self) -> ComposeResult:
            yield VerticalScroll(
                Static(
                    "No compilation output yet. Run a query to see assembly details.",
                    classes="cv-placeholder",
                    id="cv-placeholder",
                ),
                id="cv-scroll",
            )
            yield Static("", classes="cv-footer", id="cv-footer")

        def _on_state_changed(self) -> None:
            if not self.is_mounted:
                return
            with contextlib.suppress(Exception):
                self._refresh_content()

        def _refresh_content(self) -> None:
            if self._placeholder:
                self._render_placeholder()
            elif self._errors:
                self._render_errors()
            elif self._assembly is not None:
                self._render_assembly_visual()

        def _render_placeholder(self) -> None:
            scroll = self.query_one("#cv-scroll", VerticalScroll)
            self._clear_scroll(scroll)
            scroll.mount(Static(
                "No compilation output yet. Run a query to see assembly details.",
                classes="cv-placeholder",
                id="cv-placeholder",
            ))
            self.query_one("#cv-footer", Static).update("")

        def _render_errors(self) -> None:
            scroll = self.query_one("#cv-scroll", VerticalScroll)
            self._clear_scroll(scroll)
            for err in self._errors:
                scroll.mount(Static(f"[red]\u2717 {err}[/]", classes="cv-error"))
            self.query_one("#cv-footer", Static).update(
                f"Compilation failed \u2014 {len(self._errors)} error(s)"
            )

        def _render_assembly_visual(self) -> None:
            """Render assembly using the canonical render_visual renderer."""
            from rivet_cli.rendering.visual import render_visual

            scroll = self.query_one("#cv-scroll", VerticalScroll)
            self._clear_scroll(scroll)
            text = render_visual(self._assembly, self._verbosity, color=False)
            for line in text.splitlines():
                scroll.mount(Static(line, classes="cv-row"))
            status = "\u2713 valid" if self._assembly.success else "\u2717 invalid"
            self.query_one("#cv-footer", Static).update(
                f"Compilation ({status}) \u2014 verbosity {self._verbosity}"
            )

        def _clear_scroll(self, scroll: VerticalScroll) -> None:
            for child in list(scroll.children):
                child.remove()

else:
    # Fallback for environments without Textual
    class CompilationView(_CompilationViewState):  # type: ignore[no-redef]
        """Headless CompilationView for testing without Textual."""

        def __init__(
            self, session: Any | None = None, **kwargs: Any
        ) -> None:
            self._init_state(session)
