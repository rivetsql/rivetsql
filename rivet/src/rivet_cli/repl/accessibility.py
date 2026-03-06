"""Accessibility support for the Rivet REPL TUI.

Provides ARIA-style label constants and tab-order helpers following
Textual's built-in accessibility features.

Requirements: 35.1, 35.2, 35.3, 35.4
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# ARIA-style label constants for all interactive elements
#
# Every constant below is imported and assigned as a Textual widget
# ``tooltip`` at mount time.  Removing any constant will break the
# corresponding widget's accessibility label.
# ---------------------------------------------------------------------------

# Panel labels  (used in app.py — tooltip on top-level layout panels)
ARIA_CATALOG_PANEL = "Catalog panel — browse source catalogs and pipeline joints"
ARIA_EDITOR_PANEL = "SQL editor panel — write and execute queries"
ARIA_RESULTS_PANEL = "Results panel — view query results, diffs, and profiles"
ARIA_STATUS_BAR = "Status bar — active profile, engine, and compilation status"

# Catalog panel elements  (used in widgets/catalog.py — tooltip on tree & search input)
ARIA_CATALOG_TREE = "Catalog tree — navigate with arrow keys, Enter to open, F4 to query"
ARIA_CATALOG_SEARCH = "Catalog search — type to filter catalog entries"

# Editor panel elements  (used in widgets/editor.py — tooltip on editor widgets)
ARIA_EDITOR_AREA = "SQL editor — write queries, F5 to execute, Ctrl+Space for autocomplete"
ARIA_TAB_BAR = "Editor tabs — Ctrl+Tab / Ctrl+Shift+Tab to switch, Ctrl+N for new tab"
ARIA_FIND_INPUT = "Find — type to search in editor"
ARIA_REPLACE_INPUT = "Replace — type replacement text"
ARIA_AUTOCOMPLETE_LIST = "Autocomplete suggestions — Up/Down to navigate, Tab or Enter to accept, Escape to dismiss"

# Results panel elements  (used in widgets/results.py — tooltip on table, tabs, footer)
ARIA_RESULTS_TABLE = "Results table — arrow keys to navigate, Ctrl+C to copy, Enter to select"
ARIA_RESULTS_TABS = "Result set tabs — navigate between multiple result sets"
ARIA_RESULTS_FOOTER = "Results summary — row count, column count, elapsed time"

# Command input  (used in widgets/command_input.py — tooltip on command input widget)
ARIA_COMMAND_INPUT = "Command input — type colon commands, Tab to complete, Escape to dismiss"

# Screen overlays  (used in screens/command_palette.py — tooltip on palette screen)
ARIA_COMMAND_PALETTE = "Command palette — type to search commands, Enter to execute, Escape to close"

# ---------------------------------------------------------------------------
# Tab order: CatalogPanel → EditorPanel → ResultsPanel → Footer
# (Requirement 35.2)
#
# Used in app.py for Tab/Shift+Tab panel focus cycling.
# ---------------------------------------------------------------------------

# CSS selector order for Tab/Shift+Tab panel cycling
PANEL_TAB_ORDER: list[str] = [
    "#catalog-panel",
    "#editor-panel",
    "#results-panel",
]
