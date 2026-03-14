"""Generic file format registry for Rivet plugins.

Provides a canonical ``FileFormat`` enum, extension mappings, format detection
(including directory probing for local dirs and S3 prefixes), cascading
resolution, validation, and per-plugin capability declarations.

All four file-oriented plugins (filesystem_catalog, filesystem_sink,
s3_source, s3_sink) delegate format handling to this module.
"""

from __future__ import annotations

import os
from collections import Counter
from collections.abc import Sequence
from enum import Enum
from pathlib import Path

from rivet_core.errors import PluginValidationError, plugin_error

# ---------------------------------------------------------------------------
# FileFormat enum
# ---------------------------------------------------------------------------


class FileFormat(str, Enum):  # noqa: UP042 — str,Enum for Python 3.10 compat
    """Canonical set of data file formats recognised by Rivet.

    Values: PARQUET, CSV, JSON, IPC, ORC, DELTA.
    Inherits from ``str`` (not ``StrEnum``) for Python 3.10 compatibility.
    """

    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    IPC = "ipc"
    ORC = "orc"
    DELTA = "delta"


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------


EXT_TO_FORMAT: dict[str, FileFormat] = {
    ".parquet": FileFormat.PARQUET,
    ".pq": FileFormat.PARQUET,
    ".csv": FileFormat.CSV,
    ".tsv": FileFormat.CSV,
    ".json": FileFormat.JSON,
    ".jsonl": FileFormat.JSON,
    ".ndjson": FileFormat.JSON,
    ".arrow": FileFormat.IPC,
    ".feather": FileFormat.IPC,
    ".ipc": FileFormat.IPC,
    ".orc": FileFormat.ORC,
}
"""Extension → FileFormat.  Case-insensitive lookup is done at call time."""

FORMAT_TO_EXT: dict[FileFormat, str] = {
    FileFormat.PARQUET: ".parquet",
    FileFormat.CSV: ".csv",
    FileFormat.JSON: ".json",
    FileFormat.IPC: ".arrow",
    FileFormat.ORC: ".orc",
    FileFormat.DELTA: "",  # delta uses a directory, no single extension
}
"""FileFormat → primary file extension (for output / sink use)."""

PLUGIN_CAPABILITIES: dict[tuple[str, str], frozenset[FileFormat]] = {
    ("filesystem", "catalog"): frozenset(
        {FileFormat.PARQUET, FileFormat.CSV, FileFormat.JSON, FileFormat.IPC}
    ),
    ("filesystem", "source"): frozenset(
        {FileFormat.PARQUET, FileFormat.CSV, FileFormat.JSON, FileFormat.IPC}
    ),
    ("filesystem", "sink"): frozenset(
        {FileFormat.PARQUET, FileFormat.CSV, FileFormat.JSON, FileFormat.IPC}
    ),
    ("s3", "source"): frozenset(
        {FileFormat.PARQUET, FileFormat.CSV, FileFormat.JSON, FileFormat.ORC, FileFormat.DELTA}
    ),
    ("s3", "sink"): frozenset(
        {FileFormat.PARQUET, FileFormat.CSV, FileFormat.JSON, FileFormat.ORC, FileFormat.DELTA}
    ),
}
"""(plugin_name, plugin_type) → supported formats."""

# Pre-compute a lowercase lookup for validate_format
_NAME_TO_FORMAT: dict[str, FileFormat] = {f.value: f for f in FileFormat}


# ---------------------------------------------------------------------------
# FormatRegistry
# ---------------------------------------------------------------------------


class FormatRegistry:
    """Central registry for file format detection, resolution, and validation.

    All methods are ``@staticmethod`` — no instantiation needed.  The registry
    has zero I/O dependencies: local directory probing uses ``pathlib``, and
    S3 probing is handled by the caller passing ``child_names``.
    """

    @staticmethod
    def detect_format(
        path: str | Path,
        default: FileFormat = FileFormat.PARQUET,
        *,
        child_names: Sequence[str] | None = None,
    ) -> FileFormat:
        """Detect format from a file path, local directory, or S3 prefix.

        Resolution order:

        1. If *path* has a recognised file extension → return that format.
        2. If *path* is a local directory (``Path.is_dir()``):

           a. ``_delta_log/`` subdirectory exists → ``DELTA``.
           b. Scan immediate children → most-common recognised extension.
           c. No recognised children → *default*.

        3. If *child_names* is provided (S3 prefixes / non-local paths):

           a. Any child matches ``_delta_log`` or ``_delta_log/`` → ``DELTA``.
           b. Scan child-name extensions → most-common recognised format.
           c. No recognised children → *default*.

        4. Otherwise → *default*.

        Extension matching is case-insensitive.

        Args:
            path: File path, directory path, or S3 key.
            default: Fallback format when detection is inconclusive.
            child_names: Optional list of child basenames (for S3 probing).

        Returns:
            The detected ``FileFormat``.
        """
        p = Path(path) if not isinstance(path, Path) else path

        # 1. Extension lookup (case-insensitive)
        ext = p.suffix.lower()
        if ext in EXT_TO_FORMAT:
            return EXT_TO_FORMAT[ext]

        # 2. Local directory probing
        if p.is_dir():
            return _probe_children_from_dir(p, default)

        # 3. S3 / remote probing via caller-supplied child names
        if child_names is not None:
            return _probe_children(child_names, default)

        # 4. Fallback
        return default

    @staticmethod
    def resolve_format(
        *candidates: str | None,
        path: str | Path | None = None,
        default: FileFormat = FileFormat.PARQUET,
        child_names: Sequence[str] | None = None,
    ) -> FileFormat:
        """Cascading resolution: first non-empty valid candidate wins.

        Priority order:

        1. First non-``None``, non-empty string in *candidates* (parsed
           case-insensitively).
        2. Path-based detection (extension for files, directory probing for
           directories), delegated to :meth:`detect_format`.
        3. *default*.

        Args:
            *candidates: Ordered format name strings (may be ``None`` or ``""``).
            path: Optional file/directory path for detection fallback.
            default: Ultimate fallback format.
            child_names: Passed through to :meth:`detect_format` for S3 probing.

        Returns:
            The resolved ``FileFormat``.
        """
        for c in candidates:
            if c is not None and c != "":
                low = c.strip().lower()
                if low in _NAME_TO_FORMAT:
                    return _NAME_TO_FORMAT[low]

        if path is not None:
            return FormatRegistry.detect_format(path, default, child_names=child_names)

        return default

    @staticmethod
    def validate_format(name: str) -> FileFormat:
        """Parse and validate a format name string (case-insensitive).

        Args:
            name: Format name to validate.

        Returns:
            The corresponding ``FileFormat``.

        Raises:
            PluginValidationError: If *name* is not a recognised format.
        """
        low = name.strip().lower()
        fmt = _NAME_TO_FORMAT.get(low)
        if fmt is not None:
            return fmt
        valid = ", ".join(sorted(f.value for f in FileFormat))
        raise PluginValidationError(
            plugin_error(
                "RVT-202",
                f"Invalid format '{name}'.",
                plugin_name="format_registry",
                plugin_type="registry",
                remediation=f"Supported formats: {valid}",
            )
        )

    @staticmethod
    def validate_plugin_support(
        fmt: FileFormat,
        plugin_name: str,
        plugin_type: str,
    ) -> None:
        """Raise if *plugin_name*/*plugin_type* does not support *fmt*.

        Args:
            fmt: The format to check.
            plugin_name: Plugin identifier (e.g. ``"filesystem"``).
            plugin_type: One of ``"catalog"``, ``"source"``, ``"sink"``.

        Raises:
            PluginValidationError: If the format is unsupported.
        """
        key = (plugin_name, plugin_type)
        caps = PLUGIN_CAPABILITIES.get(key)
        if caps is not None and fmt not in caps:
            supported = ", ".join(sorted(f.value for f in caps))
            raise PluginValidationError(
                plugin_error(
                    "RVT-202",
                    f"Format '{fmt.value}' is not supported by {plugin_name} {plugin_type}.",
                    plugin_name=plugin_name,
                    plugin_type=plugin_type,
                    remediation=(f"Supported formats for {plugin_name} {plugin_type}: {supported}"),
                )
            )

    @staticmethod
    def is_supported(fmt: FileFormat, plugin_name: str, plugin_type: str) -> bool:
        """Check whether a plugin supports a format.

        Args:
            fmt: The format to query.
            plugin_name: Plugin identifier.
            plugin_type: Plugin role.

        Returns:
            ``True`` if supported (or if the plugin has no capability entry),
            ``False`` otherwise.
        """
        key = (plugin_name, plugin_type)
        caps = PLUGIN_CAPABILITIES.get(key)
        if caps is None:
            return True
        return fmt in caps

    @staticmethod
    def primary_extension(fmt: FileFormat) -> str:
        """Return the primary file extension for *fmt* (e.g. ``'.parquet'``).

        Args:
            fmt: The file format.

        Returns:
            Extension string including the leading dot, or ``""`` for DELTA.
        """
        return FORMAT_TO_EXT.get(fmt, "")


# ---------------------------------------------------------------------------
# Private helpers for directory probing
# ---------------------------------------------------------------------------


def _probe_children_from_dir(directory: Path, default: FileFormat) -> FileFormat:
    """Probe a local directory's immediate children to infer format."""
    # Delta detection: _delta_log/ subdirectory
    if (directory / "_delta_log").is_dir():
        return FileFormat.DELTA

    child_exts: list[str] = []
    try:
        for entry in os.scandir(str(directory)):
            if entry.is_file():
                _, ext = os.path.splitext(entry.name)
                if ext:
                    child_exts.append(ext.lower())
    except OSError:
        return default

    return _most_common_format(child_exts, default)


def _probe_children(child_names: Sequence[str], default: FileFormat) -> FileFormat:
    """Probe a list of child basenames (e.g. from S3 listing) to infer format."""
    for name in child_names:
        stripped = name.rstrip("/")
        if stripped == "_delta_log" or name == "_delta_log/":
            return FileFormat.DELTA

    exts: list[str] = []
    for name in child_names:
        _, ext = os.path.splitext(name)
        if ext:
            exts.append(ext.lower())

    return _most_common_format(exts, default)


def _most_common_format(extensions: list[str], default: FileFormat) -> FileFormat:
    """Return the format of the most common recognised extension.

    Ties are broken by enum ordinal (lowest wins) for determinism.
    """
    counts: Counter[FileFormat] = Counter()
    for ext in extensions:
        fmt = EXT_TO_FORMAT.get(ext)
        if fmt is not None:
            counts[fmt] += 1

    if not counts:
        return default

    max_count = max(counts.values())
    # Among formats tied at max_count, pick lowest enum ordinal
    tied = [f for f, c in counts.items() if c == max_count]
    tied.sort(key=lambda f: list(FileFormat).index(f))
    return tied[0]
