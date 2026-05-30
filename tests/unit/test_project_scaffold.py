import tomllib
from pathlib import Path

import yaml

import vina_bim_shop


def test_project_scaffold_artifacts_exist() -> None:
    repo_root = Path(__file__).resolve().parents[2]

    assert vina_bim_shop.__doc__ == "Vina Bim Shop coursework package."
    assert (repo_root / "architecture" / "masterplan.md").is_file()
    assert (repo_root / "configs" / "generator" / "base.yaml").is_file()
    assert (
        repo_root / "data" / "reference" / "taxonomy" / "taxonomy_snapshot.yaml"
    ).is_file()


def test_duckdb_is_not_a_project_dependency_or_pipeline_engine() -> None:
    repo_root = Path(__file__).resolve().parents[2]

    pyproject = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = [dependency.lower() for dependency in pyproject["project"]["dependencies"]]

    assert all(not dependency.startswith("duckdb") for dependency in dependencies)
    assert "duckdb" not in (repo_root / "uv.lock").read_text(encoding="utf-8").lower()

    pipeline_config = yaml.safe_load(
        (repo_root / "configs" / "pipelines" / "local.yaml").read_text(encoding="utf-8")
    )

    assert pipeline_config["batch_engine"] == "spark"
    assert pipeline_config["stream_engine"] == "flink"
    assert pipeline_config["sql_engine"] == "trino"
    assert pipeline_config["catalog"] == "hive_metastore"
    assert pipeline_config["object_store"] == "minio"
    assert "engine" not in pipeline_config
    assert "duckdb_path" not in pipeline_config["storage"]
