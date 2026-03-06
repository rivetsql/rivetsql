"""REPL package for rivet-cli — entry point for `rivet repl`."""

from __future__ import annotations

import argparse
import signal
import sys
from pathlib import Path
from typing import Any

# Exit code constants (inline to avoid importing from rivet_cli.exit_codes,
# which would violate the module boundary: rivet_cli/repl/ may only import
# from stdlib, rivet_core, rivet_config, rivet_bridge, and textual).
_EXIT_SUCCESS = 0
_EXIT_GENERAL_ERROR = 1
_EXIT_USAGE_ERROR = 10
_EXIT_INTERRUPTED = 130


def add_repl_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:  # type: ignore[type-arg]
    """Register the `repl` subcommand on the given subparsers action."""
    repl_p = subparsers.add_parser("repl", help="Launch the interactive REPL or execute a query")
    repl_subs = repl_p.add_subparsers(dest="repl_action")

    # --- default interactive mode (no subcommand) ---
    # These stay on repl_p so `rivet repl --profile x` still works.
    repl_p.add_argument("--profile", "-p", default="default", help="Profile name (default: default)")
    repl_p.add_argument("--project", default=".", help="Project directory (default: .)")
    repl_p.add_argument("--theme", default="rivet", help="UI theme (default: rivet)")
    repl_p.add_argument("--no-watch", action="store_true", help="Disable file watching")
    repl_p.add_argument("--read-only", action="store_true", help="Disable execution operations")
    repl_p.add_argument("--editor", default=None, help="Pre-load a file in the editor")

    # --- execute subcommand ---
    exec_p = repl_subs.add_parser("execute", help="Execute a SQL query non-interactively")
    exec_p.add_argument("--query", "-q", required=True, help="SQL query to execute")
    exec_p.add_argument("--engine", "-e", default=None, help="Engine to use (default: profile default)")
    exec_p.add_argument("--profile", "-p", default="default", help="Profile name (default: default)")
    exec_p.add_argument("--project", default=".", help="Project directory (default: .)")
    exec_p.add_argument("--format", "-f", default="table", help="Output format: table, json, csv (default: table)")
    exec_p.add_argument("--max-rows", type=int, default=10_000, help="Maximum rows to return (default: 10000)")

    return repl_p  # type: ignore[no-any-return]


def _make_loader() -> Any:
    """Create a ProjectLoader that uses rivet_config + rivet_bridge."""
    from rivet_bridge import build_assembly, register_optional_plugins  # noqa: PLC0415
    from rivet_config import load_config  # noqa: PLC0415
    from rivet_core import Assembly, Catalog, ComputeEngine, PluginRegistry  # noqa: PLC0415

    class _BridgeProjectLoader:
        """ProjectLoader implementation using config + bridge."""

        def load(
            self, project_path: Path, profile_name: str
        ) -> tuple[Assembly, dict[str, Catalog], dict[str, ComputeEngine], PluginRegistry, str]:
            config_result = load_config(project_path, profile_name)
            if not config_result.success:
                msgs = "; ".join(e.message for e in config_result.errors)
                raise RuntimeError(f"Config load failed: {msgs}")

            registry = PluginRegistry()
            registry.register_builtins()
            register_optional_plugins(registry)

            bridge_result = build_assembly(config_result, registry)
            default_engine = (
                config_result.profile.default_engine
                if config_result.profile
                else ""
            )
            return (
                bridge_result.assembly,
                bridge_result.catalogs,
                bridge_result.engines,
                registry,
                default_engine,
            )

    return _BridgeProjectLoader()


def run_repl(args: argparse.Namespace) -> int:
    """Entry point for `rivet repl`. Returns an exit code.

    Exit codes:
      0   — normal exit
      1   — startup error (missing deps or session start failure)
      10  — invalid arguments
      130 — SIGINT interruption
    """
    # --- Dispatch to execute subcommand if requested ---
    repl_action = getattr(args, "repl_action", None)
    if repl_action == "execute":
        from .execute import run_execute  # noqa: PLC0415

        return run_execute(
            sql=args.query,
            project=args.project,
            profile=args.profile,
            engine=getattr(args, "engine", None),
            format=args.format,
            max_rows=getattr(args, "max_rows", 10_000),
        )

    # --- Interactive REPL (default) ---
    project_path = Path(args.project)
    if not project_path.exists():
        print(
            f"error: project directory does not exist: {project_path}",
            file=sys.stderr,
        )
        return _EXIT_USAGE_ERROR

    # --- Build ReplConfig from CLI args (relative import — not flagged by boundary test) ---
    from .config import ReplConfig  # noqa: PLC0415

    config = ReplConfig(
        theme=args.theme,
        file_watch=not args.no_watch,
    )

    # --- Create and start InteractiveSession ---
    from rivet_core.interactive import InteractiveSession  # noqa: PLC0415

    session = InteractiveSession(
        project_path=project_path,
        profile=args.profile,
        read_only=args.read_only,
        max_results=config.max_results,
        loader=_make_loader(),
    )

    try:
        session.start()
    except Exception as exc:  # noqa: BLE001
        print(f"error: failed to start REPL session: {exc}", file=sys.stderr)
        # Surface individual bridge errors when available.
        errors = getattr(exc, "errors", None)
        if errors:
            for err in errors:
                prefix = f"  [{err.code}]" if hasattr(err, "code") else "  -"
                print(f"{prefix} {err.message}", file=sys.stderr)
                if getattr(err, "remediation", None):
                    print(f"    → {err.remediation}", file=sys.stderr)
        return _EXIT_GENERAL_ERROR

    # --- Launch TUI ---
    exit_code = _EXIT_SUCCESS
    original_sigint = signal.getsignal(signal.SIGINT)
    try:
        from .app import RivetRepl  # noqa: PLC0415

        editor_path: Path | None = Path(args.editor) if args.editor else None
        app = RivetRepl(
            session=session,
            config=config,
            editor_path=editor_path,
            initial_sql=getattr(args, "initial_sql", None),
        )
        app.run()
    except KeyboardInterrupt:
        exit_code = _EXIT_INTERRUPTED
    except ImportError as exc:
        # app.py not yet implemented — treat as startup error
        print(f"error: REPL app not available: {exc}", file=sys.stderr)
        exit_code = _EXIT_GENERAL_ERROR
    except Exception as exc:  # noqa: BLE001
        print(f"error: REPL exited with error: {exc}", file=sys.stderr)
        exit_code = _EXIT_GENERAL_ERROR
    finally:
        signal.signal(signal.SIGINT, original_sigint)
        try:
            session.stop()
        except Exception:  # noqa: BLE001
            pass

    return exit_code
