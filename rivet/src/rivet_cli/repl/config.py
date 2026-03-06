"""REPL configuration parsed from rivet.yaml under the `repl` key."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class FormatterConfig:
    """SQL formatter settings."""

    indent: int = 2
    uppercase_keywords: bool = True
    trailing_commas: bool = True
    max_line_length: int = 80

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> FormatterConfig:
        return cls(
            indent=int(raw.get("indent", 2)),
            uppercase_keywords=bool(raw.get("uppercase_keywords", True)),
            trailing_commas=bool(raw.get("trailing_commas", True)),
            max_line_length=int(raw.get("max_line_length", 80)),
        )


@dataclass(frozen=True)
class ReplConfig:
    """REPL-specific configuration from rivet.yaml `repl` key.

    All fields have defaults; missing keys in rivet.yaml are silently replaced
    with the default value.
    """

    theme: str = "rivet"
    theme_path: str | None = None
    keymap: str = "vscode"
    max_results: int = 10_000
    autocomplete: bool = True
    file_watch: bool = True
    debounce_ms: int = 500
    editor_cache: bool = True
    show_line_numbers: bool = True
    show_minimap: bool = False
    tab_size: int = 2
    word_wrap: bool = False
    formatter: FormatterConfig = field(default_factory=FormatterConfig)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ReplConfig:
        """Build a ReplConfig from the `repl` mapping in rivet.yaml.

        Unknown keys are ignored; missing keys fall back to defaults.
        """
        formatter_raw = raw.get("formatter", {})
        formatter = (
            FormatterConfig.from_dict(formatter_raw)
            if isinstance(formatter_raw, dict)
            else FormatterConfig()
        )
        return cls(
            theme=str(raw.get("theme", "rivet")),
            theme_path=str(raw["theme_path"]) if raw.get("theme_path") else None,
            keymap=str(raw.get("keymap", "vscode")),
            max_results=int(raw.get("max_results", 10_000)),
            autocomplete=bool(raw.get("autocomplete", True)),
            file_watch=bool(raw.get("file_watch", True)),
            debounce_ms=int(raw.get("debounce_ms", 500)),
            editor_cache=bool(raw.get("editor_cache", True)),
            show_line_numbers=bool(raw.get("show_line_numbers", True)),
            show_minimap=bool(raw.get("show_minimap", False)),
            tab_size=int(raw.get("tab_size", 2)),
            word_wrap=bool(raw.get("word_wrap", False)),
            formatter=formatter,
        )

    @classmethod
    def from_rivet_yaml(cls, rivet_yaml: dict[str, Any]) -> ReplConfig:
        """Extract and parse the `repl` section from a parsed rivet.yaml mapping.

        If the `repl` key is absent or not a mapping, returns a default ReplConfig.
        """
        repl_raw = rivet_yaml.get("repl", {})
        if not isinstance(repl_raw, dict):
            return cls()
        return cls.from_dict(repl_raw)
