"""DuckDB extension management: check-then-load pattern."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rivet_core.errors import ExecutionError, plugin_error

if TYPE_CHECKING:
    import duckdb


def preload_extensions(conn: duckdb.DuckDBPyConnection, extensions: list[str]) -> None:
    """Pre-load a list of DuckDB extensions at engine startup.

    Calls ensure_extension for each extension in the list. Fails fast on the
    first extension that cannot be loaded.
    """
    for ext in extensions:
        ensure_extension(conn, ext)


def ensure_extension(conn: duckdb.DuckDBPyConnection, ext: str, install_from: str | None = None) -> None:
    """Check-then-load a DuckDB extension.

    1. Query duckdb_extensions() to check installed/loaded state.
    2. INSTALL (optionally FROM <source>) if not installed.
    3. LOAD the extension.

    Args:
        conn: DuckDB connection.
        ext: Extension name.
        install_from: Optional repository source, e.g. ``"community"``.

    Raises ExecutionError (RVT-502) if the extension cannot be loaded.
    """
    try:
        row = conn.execute(
            "SELECT installed, loaded FROM duckdb_extensions() WHERE extension_name = ?",
            [ext],
        ).fetchone()

        installed = row[0] if row is not None else False
        loaded = row[1] if row is not None else False

        if loaded:
            return

        if not installed:
            install_sql = f"INSTALL {ext} FROM {install_from}" if install_from else f"INSTALL {ext}"
            conn.execute(install_sql)

        conn.execute(f"LOAD {ext}")
    except ExecutionError:
        raise
    except Exception as exc:
        raise ExecutionError(
            plugin_error(
                "RVT-502",
                f"Failed to load DuckDB extension '{ext}': {exc}",
                plugin_name="rivet_duckdb",
                plugin_type="engine",
                remediation=(
                    f"Run: INSTALL {ext}; LOAD {ext}; "
                    "or set the DuckDB extension directory for offline environments."
                ),
                extension=ext,
            )
        ) from exc
