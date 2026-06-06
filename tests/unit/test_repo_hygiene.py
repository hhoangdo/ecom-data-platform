import subprocess
from pathlib import Path

import yaml


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _git_ls_files(repo_root: Path, *paths: str) -> set[str]:
    completed = subprocess.run(
        ["git", "ls-files", *paths],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=True,
    )
    return {
        line
        for line in completed.stdout.splitlines()
        if line and (repo_root / Path(line)).exists()
    }


def test_gitignore_anchors_repo_local_workspace_rules() -> None:
    repo_root = _repo_root()
    gitignore = (repo_root / ".gitignore").read_text(encoding="utf-8")

    for required_rule in ["/ai/", "/notebooks/", "/sql/", "/infra/ci/", "/infra/local/"]:
        assert required_rule in gitignore

    assert "\nsql/\n" not in gitignore


def test_generator_and_pipeline_configs_keep_only_active_local_output_paths() -> None:
    repo_root = _repo_root()
    generator_config = yaml.safe_load(
        (repo_root / "configs" / "generator" / "base.yaml").read_text(encoding="utf-8")
    )
    pipeline_config = yaml.safe_load(
        (repo_root / "configs" / "pipelines" / "local.yaml").read_text(encoding="utf-8")
    )

    assert "samples_root" not in generator_config["outputs"]
    assert set(pipeline_config["storage"]) == {
        "raw_root",
        "gold_root",
        "duckdb_parity_path",
        "duckdb_executive_mart_path",
    }


def test_data_output_dirs_use_nested_gitignore_files_instead_of_gitkeep_placeholders() -> None:
    repo_root = _repo_root()

    assert (repo_root / "data" / "raw" / ".gitignore").is_file()
    assert (repo_root / "data" / "gold" / ".gitignore").is_file()
    assert not (repo_root / "data" / "raw" / ".gitkeep").exists()
    assert not (repo_root / "data" / "gold" / ".gitkeep").exists()


def test_repo_does_not_track_internal_handoff_notes_or_runtime_logs() -> None:
    repo_root = _repo_root()
    tracked = _git_ls_files(repo_root, "artifacts/airflow/logs", "tmp/ADR08_HANDOFF.md")

    assert tracked == set()


def test_repo_does_not_track_empty_scaffolds_or_placeholder_packages() -> None:
    repo_root = _repo_root()
    disallowed = [
        "scripts/bootstrap/.gitkeep",
        "scripts/pipeline/.gitkeep",
        "data/bronze/.gitkeep",
        "data/silver/.gitkeep",
        "data/samples/.gitkeep",
        "scripts/__init__.py",
        "scripts/kafka/__init__.py",
        "scripts/datahub/__init__.py",
        "src/vina_bim_shop/pipelines/__init__.py",
        "src/vina_bim_shop/pipelines/bronze/__init__.py",
        "src/vina_bim_shop/pipelines/silver/__init__.py",
        "src/vina_bim_shop/pipelines/gold/__init__.py",
        "src/vina_bim_shop/pipelines/features/__init__.py",
    ]

    tracked = _git_ls_files(repo_root, *disallowed)
    assert tracked == set()
