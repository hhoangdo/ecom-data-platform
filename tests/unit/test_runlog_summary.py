from __future__ import annotations

import importlib.util
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_script_module():
    script_path = _repo_root() / "scripts" / "qa" / "summarize_runlog.py"
    spec = importlib.util.spec_from_file_location("summarize_runlog_script", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_summarize_runlog_writes_markdown_with_commands_failures_and_final_status(tmp_path: Path) -> None:
    module = _load_script_module()

    runlog = tmp_path / "runlog.txt"
    output = tmp_path / "summary.md"
    runlog.write_text(
        "\n".join(
            [
                "PS> rtk uv run pytest",
                "5 failed in 0.24s",
                "PS> rtk docker compose ps",
                "Exit code: 1",
                "ERROR: service unhealthy",
                "PS> rtk uv run pytest",
                "268 passed, 1 skipped in 49.71s",
            ]
        ),
        encoding="utf-8",
    )

    summary = module.summarize_runlog(runlog, output)

    text = output.read_text(encoding="utf-8")
    assert summary["final_status"] == "pass"
    assert "## Commands" in text
    assert "`rtk uv run pytest`" in text
    assert "5 failed in 0.24s" in text
    assert "ERROR: service unhealthy" in text
    assert "**Final status:** pass" in text
