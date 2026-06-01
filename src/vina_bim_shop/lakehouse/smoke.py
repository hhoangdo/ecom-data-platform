from __future__ import annotations

import subprocess
from collections.abc import Callable


RunCommand = Callable[[list[str]], str]


def _run_command(command: list[str]) -> str:
    completed = subprocess.run(command, check=True, text=True, capture_output=True)
    return completed.stdout


def run_smoke_sql(*, run_command: RunCommand = _run_command) -> str:
    return run_command(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "trino",
            "trino",
            "--file",
            "/etc/trino/sql/smoke.sql",
        ]
    )
