#!/usr/bin/env python3
"""Summarize long local platform run logs into a compact markdown report."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


COMMAND_PREFIXES = ("PS>", ">")
FAILURE_MARKERS = (" failed", "ERROR", "Error", "Traceback", "Exception", "Exit code: 1")


def _extract_commands(lines: list[str]) -> list[str]:
    commands: list[str] = []
    for line in lines:
        stripped = line.strip()
        for prefix in COMMAND_PREFIXES:
            if stripped.startswith(prefix):
                command = stripped[len(prefix) :].strip()
                if command:
                    commands.append(command)
                break
    return commands


def _extract_failures(lines: list[str]) -> list[str]:
    return [line.strip() for line in lines if any(marker in line for marker in FAILURE_MARKERS)]


def _final_status(lines: list[str]) -> str:
    for line in reversed(lines):
        if re.search(r"\b\d+\s+passed\b", line):
            return "pass"
        if re.search(r"\b\d+\s+failed\b", line) or "Exit code: 1" in line or "ERROR" in line:
            return "fail"
    return "unknown"


def summarize_runlog(runlog_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    source = Path(runlog_path)
    destination = Path(output_path)
    lines = source.read_text(encoding="utf-8", errors="replace").splitlines()

    commands = _extract_commands(lines)
    failures = _extract_failures(lines)
    final_status = _final_status(lines)

    markdown_lines = [
        f"# Runlog Summary: {source.name}",
        "",
        f"**Final status:** {final_status}",
        "",
        "## Commands",
        "",
    ]
    markdown_lines.extend(f"- `{command}`" for command in commands)
    if not commands:
        markdown_lines.append("- No shell prompt commands detected.")
    markdown_lines.extend(["", "## Failures And Warnings", ""])
    markdown_lines.extend(f"- {failure}" for failure in failures)
    if not failures:
        markdown_lines.append("- No failure markers detected.")
    markdown_lines.append("")

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("\n".join(markdown_lines), encoding="utf-8")

    return {
        "source": str(source),
        "output": str(destination),
        "final_status": final_status,
        "commands": commands,
        "failures": failures,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize a long platform run log into markdown.")
    parser.add_argument("runlog", help="Path to the run log to summarize.")
    parser.add_argument("output", help="Path to write the markdown summary.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = summarize_runlog(args.runlog, args.output)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
