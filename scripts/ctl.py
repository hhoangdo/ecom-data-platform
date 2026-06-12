#!/usr/bin/env python3
"""Thin shell-agnostic dispatcher used by the root Makefile.

The Makefile prefers Python over shell built-ins so the same recipes work
identically from bash, zsh, PowerShell, Git Bash, or WSL. This module is the
single point that wraps commands which would otherwise need shell-specific
syntax. Today that is the docker compose profile lifecycle; future wrappers
should be added as new subcommands below.

Run directly with `uv run python scripts/ctl.py <command> ...`. It is a
stdlib-only script: no new project dependencies are introduced.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from typing import Sequence


def _run(args: Sequence[str]) -> int:
    """Invoke an external command, propagating its return code."""
    completed = subprocess.run(list(args), check=False)
    return completed.returncode


def compose_up(profile: str) -> int:
    """Start a single docker compose profile in detached mode."""
    return _run(["docker", "compose", "--profile", profile, "up", "-d"])


def compose_down(profile: str) -> int:
    """Stop a single docker compose profile and remove its volumes."""
    return _run(["docker", "compose", "--profile", profile, "down", "-v"])


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ctl",
        description="Thin shell-agnostic wrapper used by the root Makefile.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    compose = sub.add_parser("compose", help="docker compose lifecycle")
    compose_sub = compose.add_subparsers(dest="subcmd", required=True)

    up = compose_sub.add_parser(
        "up", help="docker compose up -d for a single profile"
    )
    up.add_argument("profile", help="compose profile name (e.g. ingestion)")

    down = compose_sub.add_parser(
        "down", help="docker compose down -v for a single profile"
    )
    down.add_argument("profile", help="compose profile name (e.g. ingestion)")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "compose" and args.subcmd == "up":
        return compose_up(args.profile)
    if args.cmd == "compose" and args.subcmd == "down":
        return compose_down(args.profile)

    parser.error("unknown command")
    return 2


if __name__ == "__main__":
    sys.exit(main())
