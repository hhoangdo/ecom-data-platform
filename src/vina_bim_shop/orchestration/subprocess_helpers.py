"""Subprocess and HTTP helpers shared by DAG runtimes.

The runtime functions in this package need to:

- Run ``docker compose exec ...`` commands against the local stack
  (``_run_command``).
- Switch the working directory for the duration of a Spark call
  (``_working_directory``).
- Read JSON from in-cluster service endpoints such as Schema Registry,
  Kafka Connect, Trino, and Airflow webserver health
  (``_get_json``).

No DAG-specific logic lives here.
"""
from __future__ import annotations

import os
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import requests

from .paths import REPO_ROOT


def _run_command(command: list[str], *, cwd: Path | None = None) -> str:
    completed = subprocess.run(
        command,
        cwd=str(cwd or REPO_ROOT),
        check=True,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )
    return completed.stdout


@contextmanager
def _working_directory(path: Path):
    original = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original)


def _get_json(url: str) -> Any:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    if not response.content:
        return {}
    if "application/json" in response.headers.get("content-type", ""):
        return response.json()
    return {"text": response.text}
