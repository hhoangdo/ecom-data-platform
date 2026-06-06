from pathlib import Path

from vina_bim_shop.generators.config import load_generator_config


def test_load_generator_config_applies_scale_and_output_overrides(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]

    config = load_generator_config(
        repo_root / "configs" / "generator" / "base.yaml",
        scale="smoke",
        raw_root=tmp_path / "raw",
        evidence_root=tmp_path / "evidence",
        seed=123,
    )

    assert config.platform_name == "vina-bim-shop"
    assert config.random_seed == 123
    assert config.history_days == 7
    assert config.entities["customers"] > 0
    assert config.entities["orders"] > config.entities["customers"]
    assert config.raw_root == tmp_path / "raw"
    assert config.evidence_root == tmp_path / "evidence"
    assert config.category_weights["FMCG"] > config.category_weights["Fashion"]
    assert "FMCG" in config.taxonomy.level_1_categories
    assert "Health & Beauty" in config.taxonomy.subcategories["FMCG"]
    assert set(config.kafka["topics"]) == {
        "commerce_events",
        "catalog_events",
        "fulfillment_events",
        "ops_events",
    }
    assert config.kafka["event_format"] == "json"
    assert config.kafka["consumer_freshness_targets"]["executive_teams_minutes"] == 60
