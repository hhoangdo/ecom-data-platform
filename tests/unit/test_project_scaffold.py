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


def test_dbt_duckdb_section02_scaffold_is_configured() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    dbt_root = repo_root / "infra" / "analytics" / "dbt"

    pyproject = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = [dependency.lower() for dependency in pyproject["project"]["dependencies"]]

    assert any(dependency.startswith("dbt-core") for dependency in dependencies)
    assert any(dependency.startswith("dbt-duckdb") for dependency in dependencies)
    assert any(dependency.startswith("duckdb") for dependency in dependencies)
    assert dbt_root.joinpath("dbt_project.yml").is_file()
    assert dbt_root.joinpath("profiles.yml").is_file()
    assert dbt_root.joinpath("models", "gold", "obt_order_performance.sql").is_file()

    pipeline_config = yaml.safe_load(
        (repo_root / "configs" / "pipelines" / "local.yaml").read_text(encoding="utf-8")
    )

    assert pipeline_config["batch_engine"] == "spark"
    assert pipeline_config["stream_engine"] == "flink"
    assert pipeline_config["sql_engine"] == "trino"
    assert pipeline_config["catalog"] == "hive_metastore"
    assert pipeline_config["object_store"] == "minio"
    assert pipeline_config["local_transform_tool"] == "dbt-duckdb"
    assert pipeline_config["storage"]["raw_root"] == "data/raw"
    assert pipeline_config["storage"]["gold_root"] == "data/gold"
    assert pipeline_config["storage"]["duckdb_parity_path"] == "data/gold/vina_bim_shop.duckdb"
    assert pipeline_config["storage"]["duckdb_executive_mart_path"] == "data/gold/vina_bim_shop_executive.duckdb"
    assert "duckdb_mart_path" not in pipeline_config["storage"]
    assert "bronze_root" not in pipeline_config["storage"]
    assert "silver_root" not in pipeline_config["storage"]
