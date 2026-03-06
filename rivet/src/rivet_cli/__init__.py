"""rivet-cli: command-line interface for Rivet."""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Parses args, dispatches to command, returns exit code."""
    from rivet_cli.app import _main

    return _main(argv)


__all__ = ["main"]
