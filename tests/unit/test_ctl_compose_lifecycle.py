from __future__ import annotations

import importlib.util
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_ctl_module():
    script_path = _repo_root() / "scripts" / "ctl.py"
    spec = importlib.util.spec_from_file_location("ctl_script", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_compose_up_serving_uses_documented_profile_bundle_and_prebuilds_hive(monkeypatch) -> None:
    ctl = _load_ctl_module()
    calls: list[list[str]] = []

    def fake_run(args):
        calls.append(list(args))
        return 0

    monkeypatch.setattr(ctl, "_run", fake_run)

    assert ctl.compose_up("serving") == 0
    assert calls == [
        ["docker", "compose", "build", "hive-metastore-init"],
        [
            "docker",
            "compose",
            "--profile",
            "ingestion",
            "--profile",
            "lakehouse",
            "--profile",
            "streaming",
            "--profile",
            "serving",
            "up",
            "-d",
        ],
    ]


def test_compose_up_ingestion_stays_single_profile_without_hive_prebuild(monkeypatch) -> None:
    ctl = _load_ctl_module()
    calls: list[list[str]] = []

    def fake_run(args):
        calls.append(list(args))
        return 0

    monkeypatch.setattr(ctl, "_run", fake_run)

    assert ctl.compose_up("ingestion") == 0
    assert calls == [["docker", "compose", "--profile", "ingestion", "up", "-d"]]


def test_compose_down_governance_uses_documented_profile_bundle(monkeypatch) -> None:
    ctl = _load_ctl_module()
    calls: list[list[str]] = []

    def fake_run(args):
        calls.append(list(args))
        return 0

    monkeypatch.setattr(ctl, "_run", fake_run)

    assert ctl.compose_down("governance") == 0
    assert calls == [
        [
            "docker",
            "compose",
            "--profile",
            "ingestion",
            "--profile",
            "lakehouse",
            "--profile",
            "governance",
            "down",
            "-v",
        ]
    ]


def test_compose_up_stops_when_hive_prebuild_fails(monkeypatch) -> None:
    ctl = _load_ctl_module()
    calls: list[list[str]] = []

    def fake_run(args):
        calls.append(list(args))
        return 17

    monkeypatch.setattr(ctl, "_run", fake_run)

    assert ctl.compose_up("lakehouse") == 17
    assert calls == [["docker", "compose", "build", "hive-metastore-init"]]
