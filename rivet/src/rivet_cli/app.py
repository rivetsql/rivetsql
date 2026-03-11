"""Command router and global options for rivet-cli."""

from __future__ import annotations

import argparse
import importlib.metadata
import os
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class GlobalOptions:
    """Resolved global options for all commands."""

    profile: str = "default"
    project_path: Path = Path(".")
    verbosity: int = 0  # 0=normal, 1=verbose, 2=debug; -1=quiet
    color: bool = True


def resolve_globals(args: argparse.Namespace) -> GlobalOptions:
    """Resolve global options from args + environment variables.

    Priority: CLI flags > environment variables > defaults.
    Environment variables: RIVET_PROFILE, RIVET_PROJECT, RIVET_NO_COLOR.
    """
    # Profile: flag > env > default
    profile = getattr(args, "profile", None)
    if profile is None:
        profile = os.environ.get("RIVET_PROFILE", "default")

    # Project path: flag > env > default
    project = getattr(args, "project", None)
    if project is None:
        project = os.environ.get("RIVET_PROJECT")
    project_path = Path(project) if project is not None else Path(".")

    # Verbosity: -q sets -1, -v increments (0/1/2)
    quiet = getattr(args, "quiet", False)
    verbose_count = getattr(args, "verbose", 0) or 0
    verbosity = -1 if quiet else min(verbose_count, 2)

    # Color: --no-color flag > RIVET_NO_COLOR env > default (True)
    no_color_flag = getattr(args, "no_color", False)
    if no_color_flag or os.environ.get("RIVET_NO_COLOR") == "1":
        color = False
    else:
        color = True

    return GlobalOptions(
        profile=profile,
        project_path=project_path,
        verbosity=verbosity,
        color=color,
    )


def _add_global_options(parser: argparse.ArgumentParser) -> None:
    """Add global options shared by all commands."""
    parser.add_argument("--profile", "-p", default=None, help="Profile name")
    parser.add_argument("--project", default=None, help="Project directory path")
    parser.add_argument("--verbose", "-v", action="count", default=0, help="Increase verbosity")
    parser.add_argument("--quiet", "-q", action="store_true", help="Suppress non-error output")
    parser.add_argument("--no-color", action="store_true", help="Disable colored output")


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser with all commands and global options."""
    parser = argparse.ArgumentParser(prog="rivet", description="Rivet data pipeline CLI")
    parser.add_argument(
        "--version",
        action="version",
        version=f"rivet {importlib.metadata.version('rivetsql')}",
    )
    _add_global_options(parser)

    subs = parser.add_subparsers(dest="command")

    # --- compile ---
    compile_p = subs.add_parser("compile", help="Compile the project")
    _add_global_options(compile_p)
    compile_p.add_argument("sink_name", nargs="?", default=None, help="Sink to compile")
    compile_p.add_argument(
        "--format", "-f", default="visual", help="Output format (visual/json/mermaid)"
    )
    compile_p.add_argument(
        "--tag", "-t", action="append", default=[], help="Filter by tag (repeatable)"
    )
    compile_p.add_argument(
        "--tag-all", action="store_true", help="Require all tags (AND semantics)"
    )
    compile_p.add_argument("--output", "-o", default=None, help="Write compiled JSON to file")

    # --- run ---
    run_p = subs.add_parser("run", help="Compile and execute the pipeline")
    _add_global_options(run_p)
    run_p.add_argument("sink_name", nargs="?", default=None, help="Sink to run")
    run_p.add_argument(
        "--fail-fast",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Stop on first failure",
    )
    run_p.add_argument(
        "--tag", "-t", action="append", default=[], help="Filter by tag (repeatable)"
    )
    run_p.add_argument("--tag-all", action="store_true", help="Require all tags (AND semantics)")
    run_p.add_argument("--format", default="text", help="Output format (text/json/quiet)")

    # --- test ---
    test_p = subs.add_parser("test", help="Run tests against fixtures")
    _add_global_options(test_p)
    test_p.add_argument("file_paths", nargs="*", default=[], help="Test file paths to run")
    test_p.add_argument(
        "--tag", "-t", action="append", default=[], help="Filter by tag (repeatable)"
    )
    test_p.add_argument("--tag-all", action="store_true", help="Require all tags (AND semantics)")
    test_p.add_argument("--target", default=None, help="Filter tests by target joint")
    test_p.add_argument("--update-snapshots", action="store_true", help="Update snapshot files")
    test_p.add_argument(
        "--fail-fast",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Stop on first failure",
    )
    test_p.add_argument(
        "--format", "-f", default="text", choices=["text", "json"], help="Output format"
    )

    # --- doctor ---
    doctor_p = subs.add_parser("doctor", help="Run project health checks")
    _add_global_options(doctor_p)
    doctor_p.add_argument(
        "--check-connections", action="store_true", help="Test catalog connectivity"
    )
    doctor_p.add_argument("--check-schemas", action="store_true", help="Check schema drift")

    # --- init ---
    init_p = subs.add_parser("init", help="Scaffold a new Rivet project")
    _add_global_options(init_p)
    init_p.add_argument(
        "directory",
        nargs="?",
        default=None,
        help="Project directory (created if missing, defaults to current dir)",
    )
    init_p.add_argument(
        "--bare", action="store_true", help="Create directory structure only, no example files"
    )
    init_p.add_argument(
        "--style",
        choices=["mixed", "sql", "yaml"],
        default="mixed",
        help="Declaration style for examples (default: mixed — YAML sources/sinks, SQL joints)",
    )

    # --- catalog ---
    cat_p = subs.add_parser("catalog", help="Browse and inspect catalog metadata")
    _add_global_options(cat_p)
    cat_subs = cat_p.add_subparsers(dest="catalog_action")

    cat_list = cat_subs.add_parser("list", help="List catalogs and their contents")
    _add_global_options(cat_list)
    cat_list.add_argument(
        "path",
        nargs="?",
        default=None,
        help="Dot-separated path (e.g., mycatalog.myschema.mytable)",
    )
    cat_list.add_argument(
        "--depth",
        "-d",
        type=int,
        default=0,
        help="Expansion depth (0=catalogs, 1=schemas, 2=tables, 3=columns)",
    )
    cat_list.add_argument(
        "--format", "-f", default="text", choices=["text", "tree", "json"], help="Output format"
    )

    cat_describe = cat_subs.add_parser("describe", help="Describe a table's schema and metadata")
    _add_global_options(cat_describe)
    cat_describe.add_argument("path", help="Table path (catalog.schema.table)")
    cat_describe.add_argument(
        "--stats", action="store_true", help="Include column-level statistics"
    )
    cat_describe.add_argument(
        "--format", "-f", default="text", choices=["text", "json"], help="Output format"
    )

    cat_search = cat_subs.add_parser("search", help="Fuzzy search across all catalogs")
    _add_global_options(cat_search)
    cat_search.add_argument("query", help="Search query")
    cat_search.add_argument("--limit", "-l", type=int, default=20, help="Maximum results")
    cat_search.add_argument(
        "--format", "-f", default="text", choices=["text", "json"], help="Output format"
    )

    cat_generate = cat_subs.add_parser("generate", help="Generate a source joint declaration")
    _add_global_options(cat_generate)
    cat_generate.add_argument("path", help="Table path (catalog.schema.table)")
    cat_generate.add_argument(
        "--format", "-f", default="yaml", choices=["yaml", "sql"], help="Output format"
    )
    cat_generate.add_argument("--output", "-o", default=None, help="Output file path")
    cat_generate.add_argument(
        "--stdout", action="store_true", help="Print to stdout instead of writing file"
    )
    cat_generate.add_argument("--name", default=None, help="Override auto-generated name")
    cat_generate.add_argument("--columns", default=None, help="Comma-separated column list")

    cat_create = cat_subs.add_parser("create", help="Create a new catalog configuration")
    _add_global_options(cat_create)
    cat_create.add_argument("--type", dest="catalog_type", default=None, help="Catalog plugin type")
    cat_create.add_argument("--name", dest="catalog_name", default=None, help="Catalog name")
    cat_create.add_argument(
        "--option", action="append", default=[], help="Catalog option (key=value, repeatable)"
    )
    cat_create.add_argument(
        "--credential",
        action="append",
        default=[],
        help="Credential option (key=value, repeatable)",
    )
    cat_create.add_argument("--no-test", action="store_true", help="Skip connection test")
    cat_create.add_argument("--dry-run", action="store_true", help="Preview YAML without writing")

    # --- engine ---
    eng_p = subs.add_parser("engine", help="Manage compute engines")
    _add_global_options(eng_p)
    eng_subs = eng_p.add_subparsers(dest="engine_action")

    eng_list = eng_subs.add_parser("list", help="List engines in the current profile")
    _add_global_options(eng_list)
    eng_list.add_argument("engine_name", nargs="?", default=None, help="Engine to show details for")
    eng_list.add_argument(
        "--format", "-f", default="text", choices=["text", "json"], help="Output format"
    )

    eng_create = eng_subs.add_parser("create", help="Create a new engine configuration")
    _add_global_options(eng_create)
    eng_create.add_argument("--type", dest="engine_type", default=None, help="Engine plugin type")
    eng_create.add_argument("--name", dest="engine_name", default=None, help="Engine name")
    eng_create.add_argument(
        "--catalog", action="append", default=[], help="Catalog to associate (repeatable)"
    )
    eng_create.add_argument(
        "--option", action="append", default=[], help="Engine option (key=value, repeatable)"
    )
    eng_create.add_argument(
        "--credential",
        action="append",
        default=[],
        help="Credential option (key=value, repeatable)",
    )
    eng_create.add_argument("--set-default", action="store_true", help="Set as the default engine")
    eng_create.add_argument("--dry-run", action="store_true", help="Preview YAML without writing")

    # --- repl ---
    from rivet_cli.repl import add_repl_parser

    add_repl_parser(subs)

    # --- explore ---
    explore_p = subs.add_parser("explore", help="Launch interactive catalog explorer")
    _add_global_options(explore_p)

    # --- watermark ---
    wm_p = subs.add_parser("watermark", help="Manage incremental watermarks")
    _add_global_options(wm_p)
    wm_subs = wm_p.add_subparsers(dest="watermark_action")

    wm_subs.add_parser("list", help="List all watermarks")

    wm_reset = wm_subs.add_parser("reset", help="Reset a joint watermark")
    wm_reset.add_argument("joint_name", help="Joint to reset")

    wm_set = wm_subs.add_parser("set", help="Set a joint watermark")
    wm_set.add_argument("joint_name", help="Joint name")
    wm_set.add_argument("value", help="Watermark value")

    return parser


def _main(argv: list[str] | None = None) -> int:
    """CLI entry point. Parses args, dispatches to command, returns exit code."""
    import signal
    import time

    from rivet_cli.errors import CLIError, format_cli_error
    from rivet_cli.exit_codes import INTERRUPTED, USAGE_ERROR
    from rivet_cli.metrics import CLIMetrics, record_metrics

    # Handle SIGINT gracefully
    def _sigint_handler(signum: int, frame: object) -> None:
        raise KeyboardInterrupt

    original_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, _sigint_handler)

    start = time.monotonic()
    command = ""
    exit_code = 0

    try:
        parser = build_parser()

        # No args → print help
        if argv is not None and len(argv) == 0:
            parser.print_help()
            return 0
        if argv is None and len(sys.argv) <= 1:
            parser.print_help()
            return 0

        # Parse args, catching argparse errors for invalid flags (RVT-852)
        try:
            args = parser.parse_args(argv)
        except SystemExit as exc:
            # argparse calls sys.exit on error or --help/--version
            code = exc.code if isinstance(exc.code, int) else 1
            if code == 0:
                return 0
            # Invalid flag/option
            err = CLIError(
                code="RVT-852",
                message="Invalid flag or option value.",
                remediation="Run 'rivet --help' or 'rivet <command> --help' for usage.",
            )
            print(format_cli_error(err, color=True), file=sys.stderr)
            return USAGE_ERROR

        command = args.command or ""
        globals_ = resolve_globals(args)

        # No command → print help
        if not command:
            parser.print_help()
            return 0

        # Dispatch to command handler
        exit_code = _dispatch(command, args, globals_)

    except KeyboardInterrupt:
        exit_code = INTERRUPTED
    finally:
        signal.signal(signal.SIGINT, original_handler)
        elapsed_ms = (time.monotonic() - start) * 1000
        metrics = CLIMetrics(
            command=command,
            command_duration_ms=elapsed_ms,
            exit_code=exit_code,
        )
        record_metrics(metrics)

    return exit_code


def _dispatch(command: str, args: argparse.Namespace, globals_: GlobalOptions) -> int:
    """Dispatch to the appropriate command handler."""
    from rivet_cli.errors import CLIError, format_cli_error
    from rivet_cli.exit_codes import USAGE_ERROR

    if command == "init":
        from rivet_cli.commands.init import run_init

        return run_init(
            directory=args.directory,
            bare=args.bare,
            style=args.style,
            globals=globals_,
        )

    if command == "doctor":
        from rivet_cli.commands.doctor import run_doctor

        return run_doctor(
            globals=globals_,
            check_connections=args.check_connections,
            check_schemas=args.check_schemas,
        )

    if command == "compile":
        from rivet_cli.commands.compile import run_compile

        return run_compile(
            sink_name=args.sink_name,
            tags=args.tag,
            tag_all=args.tag_all,
            format=args.format,
            output=args.output,
            globals=globals_,
        )

    if command == "run":
        from rivet_cli.commands.run import run_run

        return run_run(
            sink_name=args.sink_name,
            tags=args.tag,
            tag_all=args.tag_all,
            fail_fast=args.fail_fast,
            format=args.format,
            globals=globals_,
        )

    if command == "test":
        from rivet_cli.commands.test import run_test

        return run_test(
            tags=args.tag,
            tag_all=args.tag_all,
            target=args.target,
            file_paths=[Path(p) for p in args.file_paths] if args.file_paths else [],
            update_snapshots=args.update_snapshots,
            fail_fast=args.fail_fast,
            format=args.format,
            globals=globals_,
        )

    if command == "watermark":
        return _dispatch_watermark(args, globals_)

    if command == "catalog":
        return _dispatch_catalog(args, globals_)

    if command == "engine":
        return _dispatch_engine(args, globals_)

    if command == "explore":
        return _dispatch_explore(args, globals_)

    if command == "repl":
        from rivet_cli.repl import run_repl

        return run_repl(args)

    # Unknown command — should not happen with argparse subparsers,
    # but handle defensively
    err = CLIError(
        code="RVT-851",
        message=f"Unknown command: '{command}'.",
        remediation="Run 'rivet --help' to see available commands.",
    )
    print(format_cli_error(err, globals_.color), file=sys.stderr)
    return USAGE_ERROR


def _dispatch_watermark(args: argparse.Namespace, globals_: GlobalOptions) -> int:
    """Dispatch watermark subcommands."""
    from rivet_cli.errors import CLIError, format_cli_error
    from rivet_cli.exit_codes import USAGE_ERROR

    action = getattr(args, "watermark_action", None)
    if not action:
        err = CLIError(
            code="RVT-851",
            message="Missing watermark subcommand.",
            remediation="Available subcommands: list, reset, set.",
        )
        print(format_cli_error(err, globals_.color), file=sys.stderr)
        return USAGE_ERROR

    from rivet_cli.commands.watermark import (
        run_watermark_list,
        run_watermark_reset,
        run_watermark_set,
    )

    if action == "list":
        return run_watermark_list(globals=globals_)
    if action == "reset":
        return run_watermark_reset(joint_name=args.joint_name, globals=globals_)
    if action == "set":
        return run_watermark_set(joint_name=args.joint_name, value=args.value, globals=globals_)

    err = CLIError(
        code="RVT-851",
        message=f"Unknown watermark subcommand: '{action}'.",
        remediation="Available subcommands: list, reset, set.",
    )
    print(format_cli_error(err, globals_.color), file=sys.stderr)
    return USAGE_ERROR


def _dispatch_catalog(args: argparse.Namespace, globals_: GlobalOptions) -> int:
    """Dispatch catalog subcommands."""
    from rivet_cli.errors import CLIError, format_cli_error
    from rivet_cli.exit_codes import USAGE_ERROR

    action = getattr(args, "catalog_action", None)
    if not action:
        err = CLIError(
            code="RVT-851",
            message="Missing catalog subcommand.",
            remediation="Available subcommands: list, describe, search, generate, create.",
        )
        print(format_cli_error(err, globals_.color), file=sys.stderr)
        return USAGE_ERROR

    if action == "create":
        from rivet_cli.commands.catalog_create import run_catalog_create

        return run_catalog_create(
            catalog_type=getattr(args, "catalog_type", None),
            catalog_name=getattr(args, "catalog_name", None),
            options=getattr(args, "option", []),
            credentials=getattr(args, "credential", []),
            no_test=getattr(args, "no_test", False),
            dry_run=getattr(args, "dry_run", False),
            globals=globals_,
        )

    from rivet_cli.commands.catalog import _startup
    from rivet_core.smart_cache import CacheMode

    # catalog search needs READ_WRITE so progressive expansion can discover
    # nodes; other catalog commands stay WRITE_ONLY (live fetch, rehydrate).
    cache_mode = CacheMode.READ_WRITE if action == "search" else CacheMode.WRITE_ONLY
    explorer = _startup(globals_, cache_mode=cache_mode)
    if isinstance(explorer, int):
        return explorer

    try:
        if action == "list":
            from rivet_cli.commands.catalog import catalog_list

            return catalog_list(
                explorer=explorer,
                path=args.path,
                depth=args.depth,
                format=args.format,
                globals=globals_,
            )

        if action == "describe":
            from rivet_cli.commands.catalog import catalog_describe

            return catalog_describe(
                explorer=explorer,
                path=args.path,
                stats=args.stats,
                format=args.format,
                globals=globals_,
            )

        if action == "search":
            from rivet_cli.commands.catalog import catalog_search

            return catalog_search(
                explorer=explorer,
                query=args.query,
                limit=args.limit,
                format=args.format,
                globals=globals_,
            )

        if action == "generate":
            from rivet_cli.commands.catalog import catalog_generate

            columns = [c.strip() for c in args.columns.split(",")] if args.columns else None
            return catalog_generate(
                explorer=explorer,
                path=args.path,
                format=args.format,
                output=args.output,
                stdout=args.stdout,
                name=args.name,
                columns=columns,
                globals=globals_,
            )
    finally:
        explorer.close()

    err = CLIError(
        code="RVT-851",
        message=f"Unknown catalog subcommand: '{action}'.",
        remediation="Available subcommands: list, describe, search, generate, create.",
    )
    print(format_cli_error(err, globals_.color), file=sys.stderr)
    return USAGE_ERROR


def _dispatch_engine(args: argparse.Namespace, globals_: GlobalOptions) -> int:
    """Dispatch engine subcommands."""
    from rivet_cli.errors import CLIError, format_cli_error
    from rivet_cli.exit_codes import USAGE_ERROR

    action = getattr(args, "engine_action", None)
    if not action:
        err = CLIError(
            code="RVT-851",
            message="Missing engine subcommand.",
            remediation="Available subcommands: list, create.",
        )
        print(format_cli_error(err, globals_.color), file=sys.stderr)
        return USAGE_ERROR

    if action == "list":
        from rivet_cli.commands.engine import engine_list

        return engine_list(
            globals=globals_,
            engine_name=getattr(args, "engine_name", None),
            format=getattr(args, "format", "text"),
        )

    if action == "create":
        from rivet_cli.commands.engine_create import run_engine_create

        return run_engine_create(
            engine_type=getattr(args, "engine_type", None),
            engine_name=getattr(args, "engine_name", None),
            catalogs=getattr(args, "catalog", []),
            options=getattr(args, "option", []),
            credentials=getattr(args, "credential", []),
            set_default=getattr(args, "set_default", False),
            dry_run=getattr(args, "dry_run", False),
            globals=globals_,
        )

    err = CLIError(
        code="RVT-851",
        message=f"Unknown engine subcommand: '{action}'.",
        remediation="Available subcommands: list, create.",
    )
    print(format_cli_error(err, globals_.color), file=sys.stderr)
    return USAGE_ERROR


def _dispatch_explore(args: argparse.Namespace, globals_: GlobalOptions) -> int:
    """Dispatch explore command."""
    from rivet_cli.commands.catalog import _startup
    from rivet_cli.commands.explore import ExploreController
    from rivet_cli.exit_codes import INTERRUPTED
    from rivet_cli.rendering.explore_terminal import TerminalRenderer
    from rivet_core.smart_cache import CacheMode

    explorer = _startup(globals_, cache_mode=CacheMode.READ_WRITE)
    if isinstance(explorer, int):
        return explorer

    renderer = TerminalRenderer()
    controller = ExploreController(explorer=explorer, renderer=renderer)
    try:
        controller.run()
    except KeyboardInterrupt:
        return INTERRUPTED
    finally:
        explorer.close()

    # If the user pressed 'p' for preview, hand off to the REPL
    if controller.repl_query is not None:
        from rivet_cli.repl import run_repl

        repl_args = argparse.Namespace(
            profile=getattr(args, "profile", "default"),
            project=getattr(args, "project", None) or ".",
            theme="rivet",
            no_watch=False,
            read_only=False,
            editor=None,
            initial_sql=controller.repl_query,
        )
        return run_repl(repl_args)

    # If the user pressed 'g' and chose a format, write the source file
    if controller.generated_source is not None:
        import pathlib

        src = controller.generated_source
        project_dir = pathlib.Path(getattr(args, "project", None) or ".")
        sources_dir = project_dir / "sources"
        sources_dir.mkdir(parents=True, exist_ok=True)
        dest = sources_dir / src.suggested_filename
        dest.write_text(src.content, encoding="utf-8")
        print(f"Created {dest}")

    return 0


if __name__ == "__main__":
    sys.exit(_main())
